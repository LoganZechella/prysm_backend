"""API router for v1 endpoints."""
from fastapi import APIRouter

api_router = APIRouter()

# Import and include other routers here
# Example: api_router.include_router(auth.router, prefix="/auth", tags=["auth"]) 