"""Task for collecting events from various sources."""

import os
import logging
from typing import Optional
import asyncio

from app.services.event_collection import EventCollectionService

logger = logging.getLogger(__name__)

class EventCollectionTask:
    """Task for collecting events from various sources."""
    
    def __init__(self, scrapfly_api_key: Optional[str] = None):
        """Initialize the event collection task.
        
        Args:
            scrapfly_api_key: Optional API key for Scrapfly. If not provided,
                             will attempt to get from environment variable.
        """
        # Get API key from argument or environment
        self.scrapfly_api_key = scrapfly_api_key or os.getenv("SCRAPFLY_API_KEY")
        if not self.scrapfly_api_key:
            raise ValueError("Scrapfly API key must be provided either through constructor or SCRAPFLY_API_KEY environment variable")
            
        # Initialize service
        self.service = EventCollectionService(scrapfly_api_key=self.scrapfly_api_key)
        self._task = None
        
    async def start(self, interval_hours: int = 24):
        """Start the event collection task.
        
        Args:
            interval_hours: Hours between collection runs
        """
        if self._task is not None:
            logger.warning("Collection task is already running")
            return
            
        self._task = asyncio.create_task(self.run(interval_hours))
        
    async def stop(self):
        """Stop the event collection task."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        
    async def run(self, interval_hours: int = 24):
        """Run the event collection task.
        
        Args:
            interval_hours: Hours between collection runs
        """
        try:
            await self.service.start_collection_task(interval_hours=interval_hours)
        except Exception as e:
            logger.error(f"Error in event collection task: {str(e)}")
            raise 