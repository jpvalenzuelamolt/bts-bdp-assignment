from typing import Annotated

from fastapi import APIRouter, status, HTTPException
from fastapi.params import Query
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from bdi_api.settings import Settings

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)


class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


def get_mongo_client() -> MongoClient:
    """Create and return a MongoDB client from the configured URL."""
    return MongoClient(settings.mongo_url, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)


def get_positions_collection():
    """Get the positions collection from MongoDB."""
    client = get_mongo_client()
    db = client["bdi_aircraft"]
    return db["positions"]


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    try:
        collection = get_positions_collection()
        # Convert Pydantic model to dict
        position_dict = position.model_dump()
        collection.insert_one(position_dict)
        return {"status": "ok"}
    except ServerSelectionTimeoutError:
        raise HTTPException(status_code=500, detail="MongoDB connection failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting aircraft: {str(e)}")


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    try:
        collection = get_positions_collection()
        # Use aggregation pipeline to group by type and count
        pipeline = [
            {
                "$group": {
                    "_id": "$type",
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "type": "$_id",
                    "count": 1
                }
            },
            {
                "$sort": {"count": -1}
            }
        ]
        results = list(collection.aggregate(pipeline))
        return results
    except ServerSelectionTimeoutError:
        raise HTTPException(status_code=500, detail="MongoDB connection failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting stats: {str(e)}")


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination.

    Each result should include: icao, registration, type.
    Use MongoDB's skip() and limit() for pagination.
    """
    try:
        collection = get_positions_collection()
        skip = (page - 1) * page_size
        
        # Get distinct aircraft by finding latest position for each icao
        # and projecting only the required fields
        pipeline = [
            {
                "$sort": {"icao": 1, "timestamp": -1}
            },
            {
                "$group": {
                    "_id": "$icao",
                    "icao": {"$first": "$icao"},
                    "registration": {"$first": "$registration"},
                    "type": {"$first": "$type"}
                }
            },
            {
                "$sort": {"icao": 1}
            },
            {
                "$skip": skip
            },
            {
                "$limit": page_size
            },
            {
                "$project": {
                    "_id": 0,
                    "icao": 1,
                    "registration": 1,
                    "type": 1
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        return results
    except ServerSelectionTimeoutError:
        raise HTTPException(status_code=500, detail="MongoDB connection failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing aircraft: {str(e)}")


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    try:
        collection = get_positions_collection()
        # Find the latest document for this icao, sorted by timestamp descending
        aircraft = collection.find_one(
            {"icao": icao},
            sort=[("timestamp", -1)]
        )
        
        if aircraft is None:
            raise HTTPException(status_code=404, detail=f"Aircraft {icao} not found")
        
        # Remove MongoDB's _id field from response
        aircraft.pop("_id", None)
        return aircraft
    except HTTPException:
        raise
    except ServerSelectionTimeoutError:
        raise HTTPException(status_code=500, detail="MongoDB connection failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting aircraft: {str(e)}")


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    try:
        collection = get_positions_collection()
        # Delete all documents matching the icao
        result = collection.delete_many({"icao": icao})
        return {"deleted": result.deleted_count}
    except ServerSelectionTimeoutError:
        raise HTTPException(status_code=500, detail="MongoDB connection failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting aircraft: {str(e)}")
