import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from app.database import SessionLocal
from app.services.event_collection import EventCollectionService
import os

logger = logging.getLogger(__name__)

# Default locations to collect events from
DEFAULT_LOCATIONS = [
    {
        'name': 'San Francisco',
        'lat': 37.7749,
        'lng': -122.4194,
        'radius': 10  # km
    },
    {
        'name': 'New York',
        'lat': 40.7128,
        'lng': -74.0060,
        'radius': 10
    },
    {
        'name': 'Los Angeles',
        'lat': 34.0522,
        'lng': -118.2437,
        'radius': 10
    }
]

class EventCollectionTask:
    """Background task for collecting events"""
    
    def __init__(self):
        """Initialize the event collection task"""
        self.service = EventCollectionService()
        self.running = False
        self.locations = self._load_locations()
    
    def _load_locations(self) -> List[Dict[str, Any]]:
        """Load locations from environment or use defaults"""
        try:
            locations_str = os.getenv("EVENT_COLLECTION_LOCATIONS")
            if locations_str:
                return eval(locations_str)  # Safe since this is from env var
            return DEFAULT_LOCATIONS
        except Exception as e:
            logger.error(f"Error loading locations: {str(e)}")
            return DEFAULT_LOCATIONS
    
    async def start(self):
        """Start the event collection task"""
        if self.running:
            logger.warning("Event collection task is already running")
            return
        
        self.running = True
        while self.running:
            try:
                # Create database session
                db = SessionLocal()
                
                # Set date range for next 30 days
                date_range = {
                    'start': datetime.utcnow(),
                    'end': datetime.utcnow() + timedelta(days=30)
                }
                
                # Collect events
                new_events = await self.service.collect_events(
                    db=db,
                    locations=self.locations,
                    date_range=date_range
                )
                
                logger.info(f"Collected {new_events} new events")
                
                # Wait for 6 hours before next collection
                await asyncio.sleep(6 * 60 * 60)
                
            except Exception as e:
                logger.error(f"Error in event collection task: {str(e)}")
                await asyncio.sleep(300)  # Wait 5 minutes before retry
                
            finally:
                db.close()
    
    async def stop(self):
        """Stop the event collection task"""
        self.running = False 