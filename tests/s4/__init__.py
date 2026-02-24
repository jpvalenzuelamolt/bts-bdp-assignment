# tests/s4/test_exercise.py

from __future__ import annotations

import gzip
import json
from io import BytesIO
from unittest.mock import patch, MagicMock

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from bdi_api.app import app  # adjust this import to match your app entrypoint
from bdi_api.settings import Settings

settings = Settings()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_aircraft_payload(icao: str = "ab1234") -> bytes:
    """Creates a minimal valid .json.gz aircraft snapshot in memory."""
    payload = {
        "now": 1698796800.0,
        "aircraft": [
            {
                "hex": icao,
                "r": "N12345",
                "t": "B738",
                "lat": 40.712,
                "lon": -74.006,
                "alt_baro": 35000,
                "gs": 450,
                "emergency": "none",
            }
        ],
    }
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(payload).encode())
    return buf.getvalue()


def _populate_s3(s3_client, bucket: str, keys: list[str]) -> None:
    """Upload fake aircraft files to the mock S3 bucket."""
    for key in keys:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=_make_aircraft_payload(),
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def s3_bucket_name():
    return settings.s3_bucket or "test-bucket"


@pytest.fixture()
def mock_s3(s3_bucket_name):
    """Spins up a fake S3 environment using moto for the duration of the test."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=s3_bucket_name)
        yield s3


# ---------------------------------------------------------------------------
# Tests: download_data
# ---------------------------------------------------------------------------

class TestDownloadData:

    def test_uploads_correct_number_of_files(self, client, mock_s3, s3_bucket_name):
        """When file_limit=3, exactly 3 files should appear in S3."""

        # We mock requests.Session so we don't actually hit the internet
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = BytesIO(_make_aircraft_payload())
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        mock_session = MagicMock()
        mock_session.head.return_value = MagicMock(status_code=200)
        mock_session.get.return_value = mock_response

        with patch("bdi_api.s4.exercise.requests.Session", return_value=mock_session):
            with patch("bdi_api.s4.exercise.settings.s3_bucket", s3_bucket_name):
                response = client.post("/api/s4/aircraft/download?file_limit=3")

        assert response.status_code == 200
        assert "uploaded=3" in response.json()

        # Verify S3 actually has 3 objects
        objects = mock_s3.list_objects_v2(Bucket=s3_bucket_name, Prefix="raw/day=20231101/")
        assert objects.get("KeyCount", 0) == 3

    def test_skips_missing_files(self, client, mock_s3, s3_bucket_name):
        """Files that return 404 from the source should be skipped, not counted."""

        mock_session = MagicMock()
        # First call returns 404 (file missing), second returns 200
        mock_session.head.side_effect = [
            MagicMock(status_code=404),
            MagicMock(status_code=200),
            MagicMock(status_code=200),
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raw = BytesIO(_make_aircraft_payload())
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_session.get.return_value = mock_response

        with patch("bdi_api.s4.exercise.requests.Session", return_value=mock_session):
            with patch("bdi_api.s4.exercise.settings.s3_bucket", s3_bucket_name):
                response = client.post("/api/s4/aircraft/download?file_limit=2")

        assert response.status_code == 200
        assert "uploaded=2" in response.json()

    def test_zero_file_limit_uploads_nothing(self, client, mock_s3, s3_bucket_name):
        """file_limit=0 should do nothing and return OK."""
        with patch("bdi_api.s4.exercise.settings.s3_bucket", s3_bucket_name):
            response = client.post("/api/s4/aircraft/download?file_limit=0")

        assert response.status_code == 200
        objects = mock_s3.list_objects_v2(Bucket=s3_bucket_name, Prefix="raw/day=20231101/")
        assert objects.get("KeyCount", 0) == 0

    def test_missing_s3_bucket_raises_error(self, client):
        """If BDI_S3_BUCKET is not set, the endpoint should raise a clear error."""
        with patch("bdi_api.s4.exercise.settings.s3_bucket", ""):
            with pytest.raises(ValueError, match="Missing S3 bucket"):
                client.post("/api/s4/aircraft/download?file_limit=1")


# ---------------------------------------------------------------------------
# Tests: prepare_data
# ---------------------------------------------------------------------------

class TestPrepareData:

    def test_downloads_files_from_s3_and_prepares(self, client, mock_s3, s3_bucket_name, tmp_path):
        """Files in S3 should be pulled down locally and then S1 prepare should run."""

        # Put 2 fake files in the mock S3
        keys = [
            "raw/day=20231101/000000Z.json.gz",
            "raw/day=20231101/000005Z.json.gz",
        ]
        _populate_s3(mock_s3, s3_bucket_name, keys)

        with patch("bdi_api.s4.exercise.settings.s3_bucket", s3_bucket_name):
            with patch("bdi_api.s4.exercise.settings.raw_dir", str(tmp_path / "raw")):
                with patch("bdi_api.s4.exercise.settings.prepared_dir", str(tmp_path / "prepared")):
                    # Also patch the s1 prepare so it uses the same tmp_path
                    with patch("bdi_api.s1.exercise.settings.raw_dir", str(tmp_path / "raw")):
                        with patch("bdi_api.s1.exercise.settings.prepared_dir", str(tmp_path / "prepared")):
                            response = client.post("/api/s4/aircraft/prepare")

        assert response.status_code == 200

        # The raw files should have been downloaded locally
        raw_dir = tmp_path / "raw" / "day=20231101"
        downloaded = list(raw_dir.glob("*.json.gz"))
        assert len(downloaded) == 2

    def test_empty_bucket_returns_ok(self, client, mock_s3, s3_bucket_name, tmp_path):
        """An empty S3 bucket should not crash — just return OK."""
        with patch("bdi_api.s4.exercise.settings.s3_bucket", s3_bucket_name):
            with patch("bdi_api.s4.exercise.settings.raw_dir", str(tmp_path / "raw")):
                with patch("bdi_api.s4.exercise.settings.prepared_dir", str(tmp_path / "prepared")):
                    with patch("bdi_api.s1.exercise.settings.raw_dir", str(tmp_path / "raw")):
                        with patch("bdi_api.s1.exercise.settings.prepared_dir", str(tmp_path / "prepared")):
                            response = client.post("/api/s4/aircraft/prepare")

        assert response.status_code == 200

    def test_missing_s3_bucket_raises_error(self, client):
        """If BDI_S3_BUCKET is not set, should raise a clear error."""
        with patch("bdi_api.s4.exercise.settings.s3_bucket", ""):
            with pytest.raises(ValueError, match="Missing S3 bucket"):
                client.post("/api/s4/aircraft/prepare")

