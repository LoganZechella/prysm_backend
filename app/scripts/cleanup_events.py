#!/usr/bin/env python3

import logging
from datetime import datetime
from sqlalchemy import func
from app.db.session import SessionLocal
from app.models.event import Event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def is_valid_event(event: Event) -> bool:
    """Check if an event has all required fields"""
    return all([
        event.title,  # Must have a title
        event.source,  # Must have a source platform
        event.source_id,  # Must have a source ID
        event.start_time,  # Must have a start time
        event.url,  # Must have a URL
        event.venue and event.venue.get('name') and event.venue.get('address')  # Must have venue info
    ])

def cleanup_events():
    """Clean up invalid events from the database"""
    db = SessionLocal()
    try:
        # Get all events
        all_events = db.query(Event).all()
        logger.info(f"Found {len(all_events)} total events")
        
        # Find invalid events
        invalid_events = [event for event in all_events if not is_valid_event(event)]
        logger.info(f"Found {len(invalid_events)} invalid events")
        
        if invalid_events:
            logger.info("\nInvalid events to be removed:")
            for event in invalid_events:
                logger.info(f"  ID: {event.id}")
                logger.info(f"  Title: {event.title}")
                logger.info(f"  Source: {event.source}")
                logger.info(f"  Source ID: {event.source_id}")
                logger.info(f"  Created at: {event.created_at}")
                logger.info("")
            
            # Remove invalid events
            for event in invalid_events:
                db.delete(event)
            
            db.commit()
            logger.info(f"Successfully removed {len(invalid_events)} invalid events")
            
            # Get remaining events count
            remaining_count = db.query(Event).count()
            logger.info(f"Remaining valid events: {remaining_count}")
            
            # Get events by source
            events_by_source = db.query(Event.source, func.count(Event.id)).group_by(Event.source).all()
            logger.info("\nRemaining events by source:")
            for source, count in events_by_source:
                logger.info(f"  {source}: {count}")
        else:
            logger.info("No invalid events found")
        
    finally:
        db.close()

if __name__ == "__main__":
    cleanup_events() 