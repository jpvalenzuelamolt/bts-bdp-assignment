import os
import json
import pytest


@pytest.fixture(scope="function", autouse=True)
def setup_test_data():
    """Set up sample test data for S1 tests."""
    raw_dir = os.path.join("data", "raw", "day=20231101")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Create sample test data
    sample_data = {
        "now": 1711906500,
        "aircraft": [
            {
                "hex": "06a0af",
                "r": "N123AB",
                "t": "B738",
                "alt_baro": 28000,
                "gs": 450,
                "trace": [
                    [40.7128, -74.0060, 1711906400],
                    [40.7200, -74.0120, 1711906420],
                    [40.7300, -74.0180, 1711906440]
                ]
            },
            {
                "hex": "07b1c2",
                "r": "N456CD",
                "t": "A350",
                "alt_baro": 35000,
                "gs": 480,
                "emergency": False,
                "trace": [
                    [51.5074, -0.1278, 1711906350],
                    [51.5100, -0.1300, 1711906370]
                ]
            }
        ]
    }
    
    sample_file = os.path.join(raw_dir, "sample_data.json")
    with open(sample_file, 'w') as f:
        json.dump(sample_data, f)
    
    yield
    
    # Cleanup after test (optional)
    # if os.path.exists(raw_dir):
    #     shutil.rmtree(raw_dir)
