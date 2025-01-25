"""API router for v1 endpoints."""
from fastapi import APIRouter
from app.api import auth
from app.api.api_v1.endpoints import google

api_router = APIRouter()

# Include routers
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(google.router, tags=["google"])

# Import and include other routers here
# Example: api_router.include_router(auth.router, prefix="/auth", tags=["auth"]) 