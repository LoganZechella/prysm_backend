"""Test script for Facebook scraper."""

import asyncio
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper
from app.models.event import EventModel
from app.db.session import SessionLocal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_facebook_scraper():
    """Test Facebook scraper functionality."""
    try:
        # Load environment variables
        load_dotenv()
        scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
        
        if not scrapfly_key:
            raise ValueError("SCRAPFLY_API_KEY environment variable not set")
        
        # Initialize scraper
        scraper = FacebookScrapflyScraper(api_key=scrapfly_key)
        
        # Set up search parameters
        location = {
            "city": "Louisville",
            "state": "KY",
            "lat": 38.2527,
            "lng": -85.7585
        }
        
        date_range = {
            "start": datetime.now(),
            "end": datetime.now() + timedelta(days=7)
        }
        
        # Scrape events
        events = await scraper.scrape_events(location, date_range)
        logger.info(f"Found {len(events)} events")
        
        # Create database session
        db = SessionLocal()
        try:
            # Convert and save events
            for event_data in events:
                # Skip online events
                if event_data.get("is_online", False):
                    continue
                    
                # Create event model
                event = EventModel(
                    platform_id=f"facebook_{event_data['event_id']}",
                    title=event_data["title"],
                    description=event_data.get("description", ""),
                    start_datetime=event_data["start_time"],
                    end_datetime=event_data.get("end_time"),
                    url=event_data["url"],
                    venue_name=event_data["location"]["venue_name"],
                    venue_lat=event_data["location"].get("latitude", 0),
                    venue_lon=event_data["location"].get("longitude", 0),
                    venue_city=event_data["location"].get("city", ""),
                    venue_state=event_data["location"].get("state", ""),
                    venue_country="US",
                    organizer_name=event_data.get("organizer", ""),
                    organizer_id="",
                    platform="facebook",
                    is_online=event_data.get("is_online", False),
                    rsvp_count=event_data.get("attendance_count", 0) + event_data.get("interested_count", 0),
                    categories=event_data.get("categories", []),
                    image_url=event_data.get("image_url", "")
                )
                
                # Save to database
                db.merge(event)
            
            # Commit changes
            db.commit()
            logger.info("Successfully saved events to database")
            
        finally:
            db.close()
        
    except Exception as e:
        logger.error(f"Error testing Facebook scraper: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_facebook_scraper()) 