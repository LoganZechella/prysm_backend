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
        'city': 'louisville',
        'state': 'kentucky',
        'lat': 38.2527,
        'lng': -85.7585,
        'radius': 25  # miles
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
                        events.extend([{**event, 'platform': source_name} for event in source_events])
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
                    # Get event ID and platform
                    event_id = event_data.get('id') if isinstance(event_data, dict) else event_data.platform_id
                    platform = event_data.get('platform') if isinstance(event_data, dict) else event_data.platform
                    
                    if not event_id:
                        logger.error("Event ID is missing")
                        continue
                        
                    # Check if event already exists
                    existing = db.query(EventModel).filter_by(
                        platform=platform,
                        platform_id=event_id
                    ).first()
                    
                    if existing:
                        logger.info(f"Event {event_id} from {platform} already exists")
                        continue

                    # Create new event if not already an EventModel
                    if not isinstance(event_data, EventModel):
                        venue_data = event_data.get('venue', {})
                        organizer_data = event_data.get('organizer', {})
                        
                        event = EventModel(
                            platform_id=event_id,
                            platform=platform,
                            title=event_data['title'],
                            description=event_data.get('description', ''),
                            start_datetime=to_utc_datetime(event_data['start_time']),
                            end_datetime=to_utc_datetime(event_data['end_time']) if event_data.get('end_time') else None,
                            url=event_data['url'],
                            venue_name=venue_data.get('name', ''),
                            venue_lat=float(venue_data.get('lat', 0)),
                            venue_lon=float(venue_data.get('lon', 0)),
                            venue_city=venue_data.get('city', ''),
                            venue_state=venue_data.get('state', ''),
                            organizer_name=organizer_data.get('name', ''),
                            is_online=event_data.get('is_online', False),
                            rsvp_count=int(event_data.get('rsvp_count', 0)),
                            price_info=event_data.get('price_info', ''),
                            categories=event_data.get('categories', []),
                            image_url=event_data.get('image_url', '')
                        )
                    else:
                        event = event_data

                    db.add(event)
                    new_events_count += 1
                    logger.info(f"Added new event: {event.title} from {event.platform}")
                    
                except Exception as e:
                    logger.error(f"Error storing event {event_id}: {str(e)}")
                    continue
            
            # Commit all changes
            db.commit()
            logger.info(f"Successfully stored {new_events_count} new events")
            
            # Log statistics
            total_events = db.query(EventModel).count()
            events_by_source = db.query(
                EventModel.platform,
                func.count(EventModel.id).label('count')
            ).group_by(EventModel.platform).all()
            
            logger.info(f"Total events in database: {total_events}")
            for platform, count in events_by_source:
                logger.info(f"Events from {platform}: {count}")
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error in collect_events: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(collect_events()) 