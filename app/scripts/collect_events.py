#!/usr/bin/env python3

import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import func
from app.db.session import SessionLocal
from app.services.event_collection import EventCollectionService
from app.models.event import Event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# List of locations to scrape
LOCATIONS = [
    {
        'city': 'san francisco',
        'state': 'california',
        'lat': 37.7749,
        'lng': -122.4194,
        'radius': 25  # miles
    },
    {
        'city': 'oakland',
        'state': 'california',
        'lat': 37.8044,
        'lng': -122.2712,
        'radius': 15
    },
    {
        'city': 'san jose',
        'state': 'california',
        'lat': 37.3382,
        'lng': -121.8863,
        'radius': 20
    }
]

async def collect_events():
    """Collect events from all sources"""
    try:
        # Initialize service
        service = EventCollectionService()
        
        # Create database session
        db = SessionLocal()
        
        try:
            # Set date range for next 30 days
            date_range = {
                'start': datetime.utcnow(),
                'end': datetime.utcnow() + timedelta(days=30)
            }
            
            # Collect events
            logger.info("Starting event collection...")
            new_events_count = await service.collect_events(db, LOCATIONS, date_range)
            logger.info(f"Successfully collected {new_events_count} new events")
            
            # Get statistics
            total_events = db.query(Event).count()
            events_by_source = db.query(Event.source, func.count(Event.id)).group_by(Event.source).all()
            
            logger.info(f"Total events in database: {total_events}")
            logger.info("Events by source:")
            for source, count in events_by_source:
                logger.info(f"  {source}: {count}")
                
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error collecting events: {str(e)}")
        raise

def main():
    """Main entry point"""
    try:
        asyncio.run(collect_events())
    except KeyboardInterrupt:
        logger.info("Event collection interrupted by user")
    except Exception as e:
        logger.error(f"Event collection failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 