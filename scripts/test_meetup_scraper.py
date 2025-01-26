#!/usr/bin/env python3

import asyncio
import logging
import os
from datetime import datetime
from sqlalchemy.orm import Session
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.db.session import SessionLocal
from app.models.event import EventModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_meetup_scraper():
    """Test the Meetup scraper with Louisville events"""
    try:
        # Get Scrapfly API key
        api_key = os.getenv('SCRAPFLY_API_KEY')
        if not api_key:
            raise ValueError("SCRAPFLY_API_KEY environment variable is not set")
            
        # Initialize scraper
        scraper = MeetupScrapflyScraper(api_key=api_key)
        
        # Scrape events (default location is Louisville)
        events = await scraper.scrape_events(limit=20)  # Get 20 events
        
        logger.info(f"Successfully scraped {len(events)} events")
        
        # Print event details
        for event in events:
            logger.info(f"\nEvent: {event.title}")
            logger.info(f"Date: {event.start_datetime}")
            logger.info(f"Venue: {event.venue_name} ({event.venue_city}, {event.venue_state})")
            logger.info(f"Online: {event.is_online}")
            logger.info(f"RSVPs: {event.rsvp_count}")
            logger.info(f"URL: {event.url}")
            
        # Save to database
        db = SessionLocal()
        try:
            for event in events:
                # Check if event already exists
                existing = db.query(EventModel).filter_by(platform_id=event.platform_id).first()
                if existing:
                    logger.info(f"Event {event.title} already exists, updating...")
                    # Update fields
                    for key, value in event.__dict__.items():
                        if key != '_sa_instance_state':  # Skip SQLAlchemy internal field
                            setattr(existing, key, value)
                else:
                    logger.info(f"Adding new event: {event.title}")
                    db.add(event)
            
            db.commit()
            logger.info("Successfully saved events to database")
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error testing Meetup scraper: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_meetup_scraper()) 