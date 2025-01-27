"""
Test script for Facebook events scraper.
"""

import asyncio
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from app.scrapers import FacebookScrapflyScraper
from app.db.session import SessionLocal
from app.models.event import EventModel

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_facebook_scraper():
    """Test the Facebook scraper"""
    try:
        # Load environment variables
        load_dotenv()
        scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
        if not scrapfly_key:
            raise ValueError("SCRAPFLY_API_KEY not found in environment variables")
            
        # Check for Facebook authentication
        required_vars = ["FACEBOOK_ACCESS_TOKEN", "FACEBOOK_USER_ID", "FACEBOOK_XS_TOKEN"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        # Initialize scraper
        scraper = FacebookScrapflyScraper(api_key=scrapfly_key)
        
        # Set up search parameters
        location = {
            "city": "Louisville",
            "state": "KY",
            "lat": 38.2527,
            "lng": -85.7585,
            "radius": 25  # 25 mile radius
        }
        
        # Set date range for next week
        start_date = datetime.now()
        end_date = start_date + timedelta(days=7)
        date_range = {
            "start": start_date,
            "end": end_date
        }
        
        # Scrape events
        logger.info("Starting Facebook event scraping...")
        events = await scraper.scrape_events(location, date_range)
        logger.info(f"Found {len(events)} events")
        
        # Log event details
        for event in events:
            logger.info(f"\nEvent: {event['title']}")
            logger.info(f"Date: {event['start_datetime']}")
            logger.info(f"Venue: {event['venue_name']}")
            logger.info(f"URL: {event['url']}")
            
        # Save to database
        logger.info("Saving events to database...")
        db = SessionLocal()
        try:
            for event_data in events:
                # Create EventModel instance
                event_model = EventModel(
                    platform=event_data["platform"],
                    platform_id=event_data["platform_id"],
                    title=event_data["title"],
                    description=event_data["description"],
                    start_datetime=event_data["start_datetime"],
                    end_datetime=event_data["end_datetime"],
                    venue_name=event_data["venue_name"],
                    venue_city=event_data["venue_city"],
                    venue_state=event_data["venue_state"],
                    venue_country=event_data["venue_country"],
                    venue_lat=event_data["venue_lat"],
                    venue_lon=event_data["venue_lon"],
                    image_url=event_data["image_url"],
                    url=event_data["url"],
                    rsvp_count=event_data["rsvp_count"],
                    organizer_id=event_data["organizer_id"],
                    organizer_name=event_data["organizer_name"],
                    price_info=event_data["price_info"],
                    categories=event_data["categories"]
                )
                db.merge(event_model)  # Use merge instead of add to handle duplicates
            
            db.commit()
            logger.info("Successfully saved events to database")
            
        except Exception as e:
            logger.error(f"Error saving to database: {str(e)}")
            db.rollback()
            raise
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Error testing Facebook scraper: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(test_facebook_scraper()) 