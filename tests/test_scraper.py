import pytest
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from app.collectors.scraper import EventScraper

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_event_scraper():
    """Test the event scraping functionality"""
    scraper = EventScraper()
    
    # First test account info
    account_info = await scraper.get_account_info()
    assert account_info, "Failed to get Scrapfly account info"
    logger.info(f"Account info: {account_info}")
    
    # Test event scraping for a specific location
    location = "San Francisco, CA"
    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=7)
    
    # Test Eventbrite scraping first
    events = []
    async for event in scraper.scrape_events(
        sources=["eventbrite"],
        location=location,
        start_date=start_date,
        end_date=end_date
    ):
        events.append(event)
        logger.info(f"Found event: {event.title}")
        
    assert len(events) > 0, "No events found from Eventbrite"
    logger.info(f"Successfully scraped {len(events)} events from Eventbrite")

if __name__ == "__main__":
    pytest.main([__file__]) 