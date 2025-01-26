#!/usr/bin/env python3

import asyncio
import logging
import os
from datetime import datetime, timedelta
import pytz
from sqlalchemy import func
from app.db.session import SessionLocal
from app.services.event_collection import EventCollectionService
from app.models.event import EventModel
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

def to_utc_datetime(dt):
    """Convert datetime to UTC timezone-aware datetime"""
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    return dt

async def collect_events():
    """Collect events from all sources"""
    try:
        # Get Scrapfly API key from environment
        scrapfly_api_key = os.getenv('SCRAPFLY_API_KEY')
        if not scrapfly_api_key:
            logger.error("SCRAPFLY_API_KEY environment variable is not set. Please set it in your .env file.")
            return
        
        # Initialize event collection service
        service = EventCollectionService(scrapfly_api_key=scrapfly_api_key)
        
        # Reset circuit breakers
        await service.eventbrite.reset()
        await service.facebook.reset()
        await service.meetup.reset()
        
        # Create database session
        db = SessionLocal()
        
        try:
            # Set date range for next 30 days
            now = datetime.now(pytz.UTC)
            date_range = {
                'start': now,
                'end': now + timedelta(days=30)
            }
            
            # Collect events
            logger.info("Starting event collection...")
            events = []
            
            # Collect from each source with error handling
            for source_name, scraper in [
                ('eventbrite', service.eventbrite),
                ('facebook', service.facebook),
                ('meetup', service.meetup)
            ]:
                try:
                    logger.info(f"Collecting events from {source_name}...")
                    source_events = await scraper.scrape_events(LOCATIONS[0], date_range)
                    if source_events:
                        events.extend([{**event, 'source': source_name} for event in source_events])
                        logger.info(f"Collected {len(source_events)} events from {source_name}")
                    else:
                        logger.warning(f"No events found from {source_name}")
                except Exception as e:
                    logger.error(f"Error collecting from {source_name}: {str(e)}")
                    continue
            
            logger.info(f"Collected {len(events)} events total")
            
            # Process and store events
            new_events_count = 0
            for event_data in events:
                try:
                    # Check if event already exists
                    existing = db.query(EventModel).filter_by(
                        source=event_data['source'],
                        source_id=event_data['event_id']
                    ).first()
                    
                    if existing:
                        logger.info(f"Event {event_data['event_id']} from {event_data['source']} already exists")
                        continue
                    
                    # Create new event
                    event = EventModel(
                        title=event_data['title'],
                        description=event_data.get('description', ''),
                        start_time=to_utc_datetime(event_data['start_time']),
                        end_time=to_utc_datetime(event_data['end_time']) if event_data.get('end_time') else None,
                        location=event_data['location'],
                        categories=event_data.get('categories', []),
                        price_info=event_data.get('price_info', {}),
                        source=event_data['source'],
                        source_id=event_data['event_id'],
                        url=event_data['url'],
                        image_url=event_data.get('image_url'),
                        venue=event_data.get('venue', {}),
                        organizer=event_data.get('organizer', {}),
                        tags=event_data.get('tags', [])
                    )
                    
                    db.add(event)
                    new_events_count += 1
                    logger.info(f"Added new event: {event.title} from {event.source}")
                    
                except Exception as e:
                    logger.error(f"Error storing event {event_data.get('event_id')}: {str(e)}")
                    continue
            
            # Commit all changes
            db.commit()
            logger.info(f"Successfully stored {new_events_count} new events")
            
            # Get statistics
            total_events = db.query(EventModel).count()
            events_by_source = db.query(EventModel.source, func.count(EventModel.id)).group_by(EventModel.source).all()
            
            logger.info(f"Total events in database: {total_events}")
            logger.info("Events by source:")
            for source, count in events_by_source:
                logger.info(f"  {source}: {count}")
                
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error collecting events: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(collect_events()) 