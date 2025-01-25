"""API router for v1 endpoints."""
from fastapi import APIRouter
from app.api import auth

api_router = APIRouter()

# Include routers
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])

# Import and include other routers here
# Example: api_router.include_router(auth.router, prefix="/auth", tags=["auth"]) 