"""
Test script for Facebook scraper functionality.
"""

from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper
import asyncio
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv
import json
import pytest

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_facebook_scraper():
    """Test the Facebook scraper functionality"""
    
    # Check required environment variables
    required_vars = [
        'SCRAPFLY_API_KEY',
        'FACEBOOK_ACCESS_TOKEN',
        'FACEBOOK_USER_ID',
        'FACEBOOK_XS_TOKEN'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file")
        return
    
    # Initialize scraper
    scraper = FacebookScrapflyScraper(api_key=os.getenv('SCRAPFLY_API_KEY'))
    
    # Set up test parameters
    location = {
        'city': 'Louisville',
        'state': 'KY',
        'lat': 38.2527,
        'lng': -85.7585
    }
    
    date_range = {
        'start': datetime.now(),
        'end': datetime.now() + timedelta(days=7)
    }
    
    # Test with some categories
    categories = ['music', 'arts', 'food']
    
    try:
        logger.info("Starting Facebook scraper test")
        
        # Test URL building
        url = await scraper._build_search_url(location, date_range, categories, 1)
        logger.info(f"Generated search URL: {url}")
        assert url.startswith("https://www.facebook.com/events/search"), "Invalid search URL"
        
        # Test event scraping
        events = await scraper.scrape_events(location, date_range, categories)
        logger.info(f"Found {len(events)} events")
        
        # Validate event data
        for event in events:
            assert event.get('title'), "Event missing title"
            assert event.get('start_datetime'), "Event missing start datetime"
            assert event.get('venue_name'), "Event missing venue name"
            assert event.get('venue_lat') is not None, "Event missing venue latitude"
            assert event.get('venue_lon') is not None, "Event missing venue longitude"
            assert event.get('url'), "Event missing URL"
            assert event.get('platform_id'), "Event missing platform ID"
            
            logger.info(f"Validated event: {event['title']}")
        
        logger.info("Facebook scraper test completed successfully")
        
    except Exception as e:
        logger.error(f"Error testing scraper: {str(e)}")
        raise

if __name__ == '__main__':
    asyncio.run(test_facebook_scraper()) 