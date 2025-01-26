#!/usr/bin/env python3

import logging
from datetime import datetime
from sqlalchemy import func, cast, Float, text
from app.db.session import SessionLocal
from app.models.event import EventModel
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_events():
    """Analyze events in the database"""
    db = SessionLocal()
    try:
        # Get all events
        all_events = db.query(EventModel).all()
        
        logger.info("=== Event Database Analysis ===\n")
        
        # Get total count
        total_events = len(all_events)
        logger.info(f"Total events in database: {total_events}")
        
        # Get events by source
        source_counts = {}
        for event in all_events:
            source_counts[event.source] = source_counts.get(event.source, 0) + 1
            
        logger.info("\nEvents by source:")
        for source, count in source_counts.items():
            logger.info(f"  {source}: {count}")
        
        # Get events by date range
        now = datetime.utcnow()
        future_events = sum(1 for e in all_events if e.start_time and e.start_time >= now)
        past_events = sum(1 for e in all_events if e.start_time and e.start_time < now)
        logger.info(f"\nFuture events: {future_events}")
        logger.info(f"Past events: {past_events}")
        
        # Detailed event inspection
        logger.info("\n=== Detailed Event Data ===\n")
        for i, event in enumerate(all_events, 1):
            logger.info(f"Event {i}:")
            logger.info(f"  ID: {event.id}")
            logger.info(f"  Title: {event.title}")
            logger.info(f"  Description: {event.description[:100]}..." if event.description else "  Description: None")
            logger.info(f"  Start time: {event.start_time}")
            logger.info(f"  End time: {event.end_time}")
            logger.info(f"  Source: {event.source}")
            logger.info(f"  Source ID: {event.source_id}")
            logger.info(f"  Categories: {event.categories}")
            logger.info(f"  Price info: {json.dumps(event.price_info, indent=2)}" if event.price_info else "  Price info: None")
            logger.info(f"  Venue: {json.dumps(event.venue, indent=2)}" if event.venue else "  Venue: None")
            logger.info(f"  URL: {event.url}")
            logger.info(f"  Image URL: {event.image_url}")
            logger.info(f"  Created at: {event.created_at}")
            logger.info(f"  Updated at: {event.updated_at}")
            logger.info("")
        
    finally:
        db.close()

if __name__ == "__main__":
    analyze_events() 