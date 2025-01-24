"""API module for the application."""

from fastapi import APIRouter
from app.api.recommendations import router as recommendations_router
from app.api.preferences import router as preferences_router
from app.api.auth import router as auth_router

__all__ = [
    'recommendations_router',
    'preferences_router',
    'auth_router'
]

api_router = APIRouter()
api_router.include_router(recommendations_router, prefix="/api/recommendations", tags=["recommendations"])
api_router.include_router(preferences_router, prefix="/api/preferences", tags=["preferences"])
api_router.include_router(auth_router, prefix="/api/auth", tags=["auth"]) 