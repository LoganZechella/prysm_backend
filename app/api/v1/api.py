from fastapi import APIRouter
from app.api.v1.endpoints import recommendations, events, interactions

api_router = APIRouter()
api_router.include_router(recommendations.router, prefix="/recommendations", tags=["recommendations"])
api_router.include_router(events.router, prefix="/events", tags=["events"])
api_router.include_router(interactions.router, prefix="/interactions", tags=["interactions"]) 