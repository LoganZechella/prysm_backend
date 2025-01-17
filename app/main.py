from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
import os
from app.api import api_router
from app.tasks.event_collection import EventCollectionTask

logger = logging.getLogger(__name__)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include API routes
app.include_router(api_router)

# Event collection task instance
event_collection_task = EventCollectionTask()

@app.on_event("startup")
async def startup_event():
    """Start background tasks when the application starts"""
    # Only start event collection task if not in test mode
    if os.getenv("APP_ENV") != "test":
        asyncio.create_task(event_collection_task.start())
        logger.info("Started event collection task")
    else:
        logger.info("Skipping event collection task in test mode")

@app.on_event("shutdown")
async def shutdown_event():
    """Stop background tasks when the application shuts down"""
    await event_collection_task.stop()
    logger.info("Stopped event collection task") 