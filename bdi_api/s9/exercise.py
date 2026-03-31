from datetime import datetime
import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


def _get_sample_data() -> list[dict]:
    """Return sample pipeline data."""
    return [
        {
            "id": "run-001",
            "repository": "aircraft-tracking",
            "branch": "main",
            "status": "success",
            "triggered_by": "push",
            "started_at": "2024-03-31T14:30:00",
            "finished_at": "2024-03-31T14:45:00",
            "stages": ["lint", "test", "build", "deploy"]
        },
        {
            "id": "run-002",
            "repository": "aircraft-tracking",
            "branch": "main",
            "status": "success",
            "triggered_by": "push",
            "started_at": "2024-03-31T14:20:00",
            "finished_at": "2024-03-31T14:35:00",
            "stages": ["lint", "test", "build"]
        },
        {
            "id": "run-003",
            "repository": "aircraft-tracking",
            "branch": "develop",
            "status": "failure",
            "triggered_by": "pull_request",
            "started_at": "2024-03-31T14:10:00",
            "finished_at": "2024-03-31T14:15:00",
            "stages": ["lint", "test"]
        },
        {
            "id": "run-004",
            "repository": "data-pipeline",
            "branch": "main",
            "status": "running",
            "triggered_by": "schedule",
            "started_at": "2024-03-31T14:00:00",
            "finished_at": None,
            "stages": ["validate", "transform", "load"]
        },
        {
            "id": "run-005",
            "repository": "data-pipeline",
            "branch": "main",
            "status": "success",
            "triggered_by": "schedule",
            "started_at": "2024-03-31T13:50:00",
            "finished_at": "2024-03-31T14:05:00",
            "stages": ["validate", "transform", "load", "verify"]
        },
        {
            "id": "run-006",
            "repository": "aircraft-tracking",
            "branch": "feature/co2",
            "status": "pending",
            "triggered_by": "pull_request",
            "started_at": "2024-03-31T13:40:00",
            "finished_at": None,
            "stages": ["lint"]
        },
        {
            "id": "run-007",
            "repository": "api-server",
            "branch": "main",
            "status": "success",
            "triggered_by": "push",
            "started_at": "2024-03-31T13:30:00",
            "finished_at": "2024-03-31T13:40:00",
            "stages": ["lint", "test", "build"]
        },
        {
            "id": "run-008",
            "repository": "api-server",
            "branch": "main",
            "status": "failure",
            "triggered_by": "push",
            "started_at": "2024-03-31T13:20:00",
            "finished_at": "2024-03-31T13:25:00",
            "stages": ["lint", "test"]
        }
    ]


_PIPELINE_STAGES_DATA = {
    "run-001": [
        {"name": "lint", "status": "success", "started_at": "2024-03-31T14:30:00", "finished_at": "2024-03-31T14:32:00", "logs_url": "/api/s9/pipelines/run-001/stages/lint/logs"},
        {"name": "test", "status": "success", "started_at": "2024-03-31T14:32:00", "finished_at": "2024-03-31T14:37:00", "logs_url": "/api/s9/pipelines/run-001/stages/test/logs"},
        {"name": "build", "status": "success", "started_at": "2024-03-31T14:37:00", "finished_at": "2024-03-31T14:40:00", "logs_url": "/api/s9/pipelines/run-001/stages/build/logs"},
        {"name": "deploy", "status": "success", "started_at": "2024-03-31T14:40:00", "finished_at": "2024-03-31T14:45:00", "logs_url": "/api/s9/pipelines/run-001/stages/deploy/logs"}
    ],
    "run-002": [
        {"name": "lint", "status": "success", "started_at": "2024-03-31T14:20:00", "finished_at": "2024-03-31T14:22:00", "logs_url": "/api/s9/pipelines/run-002/stages/lint/logs"},
        {"name": "test", "status": "success", "started_at": "2024-03-31T14:22:00", "finished_at": "2024-03-31T14:28:00", "logs_url": "/api/s9/pipelines/run-002/stages/test/logs"},
        {"name": "build", "status": "success", "started_at": "2024-03-31T14:28:00", "finished_at": "2024-03-31T14:35:00", "logs_url": "/api/s9/pipelines/run-002/stages/build/logs"}
    ],
    "run-003": [
        {"name": "lint", "status": "success", "started_at": "2024-03-31T14:10:00", "finished_at": "2024-03-31T14:11:00", "logs_url": "/api/s9/pipelines/run-003/stages/lint/logs"},
        {"name": "test", "status": "failure", "started_at": "2024-03-31T14:11:00", "finished_at": "2024-03-31T14:15:00", "logs_url": "/api/s9/pipelines/run-003/stages/test/logs"}
    ],
    "run-004": [
        {"name": "validate", "status": "success", "started_at": "2024-03-31T14:00:00", "finished_at": "2024-03-31T14:02:00", "logs_url": "/api/s9/pipelines/run-004/stages/validate/logs"},
        {"name": "transform", "status": "running", "started_at": "2024-03-31T14:02:00", "finished_at": None, "logs_url": "/api/s9/pipelines/run-004/stages/transform/logs"},
        {"name": "load", "status": "pending", "started_at": "2024-03-31T14:00:00", "finished_at": None, "logs_url": "/api/s9/pipelines/run-004/stages/load/logs"}
    ],
    "run-005": [
        {"name": "validate", "status": "success", "started_at": "2024-03-31T13:50:00", "finished_at": "2024-03-31T13:52:00", "logs_url": "/api/s9/pipelines/run-005/stages/validate/logs"},
        {"name": "transform", "status": "success", "started_at": "2024-03-31T13:52:00", "finished_at": "2024-03-31T13:58:00", "logs_url": "/api/s9/pipelines/run-005/stages/transform/logs"},
        {"name": "load", "status": "success", "started_at": "2024-03-31T13:58:00", "finished_at": "2024-03-31T14:02:00", "logs_url": "/api/s9/pipelines/run-005/stages/load/logs"},
        {"name": "verify", "status": "success", "started_at": "2024-03-31T14:02:00", "finished_at": "2024-03-31T14:05:00", "logs_url": "/api/s9/pipelines/run-005/stages/verify/logs"}
    ],
    "run-006": [
        {"name": "lint", "status": "pending", "started_at": "2024-03-31T13:40:00", "finished_at": None, "logs_url": "/api/s9/pipelines/run-006/stages/lint/logs"}
    ],
    "run-007": [
        {"name": "lint", "status": "success", "started_at": "2024-03-31T13:30:00", "finished_at": "2024-03-31T13:31:00", "logs_url": "/api/s9/pipelines/run-007/stages/lint/logs"},
        {"name": "test", "status": "success", "started_at": "2024-03-31T13:31:00", "finished_at": "2024-03-31T13:36:00", "logs_url": "/api/s9/pipelines/run-007/stages/test/logs"},
        {"name": "build", "status": "success", "started_at": "2024-03-31T13:36:00", "finished_at": "2024-03-31T13:40:00", "logs_url": "/api/s9/pipelines/run-007/stages/build/logs"}
    ],
    "run-008": [
        {"name": "lint", "status": "success", "started_at": "2024-03-31T13:20:00", "finished_at": "2024-03-31T13:21:00", "logs_url": "/api/s9/pipelines/run-008/stages/lint/logs"},
        {"name": "test", "status": "failure", "started_at": "2024-03-31T13:21:00", "finished_at": "2024-03-31T13:25:00", "logs_url": "/api/s9/pipelines/run-008/stages/test/logs"}
    ]
}


def _load_pipelines_data() -> list[dict]:
    """Load pipeline data from JSON file."""
    data_file = Path(settings.local_dir) / "pipelines.json"
    
    if data_file.exists():
        try:
            with open(data_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading pipelines: {e}")
            return _get_sample_data()
    else:
        # Create sample data file if it doesn't exist
        sample_data = _get_sample_data()
        data_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(data_file, 'w') as f:
                json.dump(sample_data, f, indent=2, default=str)
        except Exception as e:
            print(f"Error saving sample data: {e}")
        return sample_data


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    """List CI/CD pipeline runs with their status.

    Returns a list of pipeline runs, optionally filtered by repository and status.
    Ordered by started_at descending (most recent first).
    Paginated with `num_results` per page and `page` number (0-indexed).

    Valid statuses: "success", "failure", "running", "pending"
    Valid triggered_by values: "push", "pull_request", "schedule", "manual"
    """
    try:
        pipelines = _load_pipelines_data()
        
        # Filter by repository if provided
        if repository:
            pipelines = [p for p in pipelines if p.get('repository') == repository]
        
        # Filter by status if provided
        if status_filter:
            pipelines = [p for p in pipelines if p.get('status') == status_filter]
        
        # Parse datetime strings and sort by started_at descending
        for p in pipelines:
            try:
                p['started_at_obj'] = datetime.fromisoformat(p['started_at'])
            except:
                p['started_at_obj'] = datetime.now()
        
        pipelines.sort(key=lambda p: p['started_at_obj'], reverse=True)
        
        # Apply pagination
        start = page * num_results
        end = start + num_results
        paginated = pipelines[start:end]
        
        # Convert to PipelineRun objects
        result = []
        for p in paginated:
            result.append(PipelineRun(
                id=p['id'],
                repository=p['repository'],
                branch=p['branch'],
                status=p['status'],
                triggered_by=p['triggered_by'],
                started_at=datetime.fromisoformat(p['started_at']),
                finished_at=datetime.fromisoformat(p['finished_at']) if p['finished_at'] else None,
                stages=p['stages']
            ))
        
        return result
    except Exception as e:
        print(f"Error listing pipelines: {e}")
        return []


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    """Get the stages of a specific pipeline run.

    Returns the stages in execution order.
    Each stage has a name, status, timestamps, and a logs URL.

    Typical stages: "lint", "test", "build", "deploy"
    """
    try:
        # Verify pipeline exists
        pipelines = _load_pipelines_data()
        pipeline_exists = any(p['id'] == pipeline_id for p in pipelines)
        
        if not pipeline_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{pipeline_id}' not found"
            )
        
        # Get stages for this pipeline
        stages_data = _PIPELINE_STAGES_DATA.get(pipeline_id, [])
        
        result = []
        for stage in stages_data:
            result.append(PipelineStage(
                name=stage['name'],
                status=stage['status'],
                started_at=datetime.fromisoformat(stage['started_at']),
                finished_at=datetime.fromisoformat(stage['finished_at']) if stage['finished_at'] else None,
                logs_url=stage['logs_url']
            ))
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting pipeline stages: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving pipeline stages"
        )
