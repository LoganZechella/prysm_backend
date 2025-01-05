from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import recommendations_router, preferences_router, auth_router
import os

app = FastAPI(
    title="Event Recommendation API",
    description="API for personalized event recommendations",
    version="1.0.0"
)

# Get CORS settings from environment
allowed_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3001").split(",")
allowed_methods = os.getenv("ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS").split(",")
allowed_headers = ["*"]  # Include Authorization header

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=allowed_methods,
    allow_headers=allowed_headers,
    expose_headers=["*"],
    max_age=3600,
)

# Include API routes
app.include_router(recommendations_router, prefix="/api/recommendations", tags=["recommendations"])
app.include_router(preferences_router, prefix="/api/preferences", tags=["preferences"])
app.include_router(auth_router, prefix="/api/auth", tags=["auth"])

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"} 