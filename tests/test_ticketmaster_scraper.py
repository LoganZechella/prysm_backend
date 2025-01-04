import pytest
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from app.scrapers.ticketmaster import TicketmasterScraper

# Load environment variables
load_dotenv()

@pytest.fixture
def scraper():
    """Create Ticketmaster scraper instance"""
    api_key = os.getenv('TICKETMASTER_API_KEY')
    return TicketmasterScraper(api_key=api_key)

@pytest.mark.asyncio
async def test_scrape_events(scraper):
    """Test scraping events for a location"""
    location = {
        "city": "San Francisco",
        "state": "CA",
        "country": "US"
    }
    
    date_range = {
        "start": datetime.utcnow(),
        "end": datetime.utcnow() + timedelta(days=30)
    }
    
    async with scraper:
        events = await scraper.scrape_events(location, date_range)
        
        assert len(events) > 0, "Should find at least one event"
        
        # Verify event structure
        event = events[0]
        assert "event_id" in event
        assert event["event_id"].startswith("tm_")
        assert "title" in event
        assert "description" in event
        assert "start_datetime" in event
        assert "location" in event
        assert "venue_name" in event["location"]
        assert "coordinates" in event["location"]
        assert "categories" in event
        assert "price_info" in event
        assert "source" in event
        assert event["source"]["platform"] == "ticketmaster"

@pytest.mark.asyncio
async def test_get_event_details(scraper):
    """Test getting detailed event information"""
    # First get an event ID from the search
    location = {
        "city": "San Francisco",
        "state": "CA",
        "country": "US"
    }
    
    async with scraper:
        events = await scraper.scrape_events(location)
        assert len(events) > 0, "Should find at least one event"
        
        event_id = events[0]["event_id"].replace("tm_", "")  # Remove our prefix
        event = await scraper.get_event_details(event_id)
        
        assert event is not None
        assert "event_id" in event
        assert "title" in event
        assert "description" in event
        assert "start_datetime" in event
        assert "location" in event
        assert "price_info" in event

@pytest.mark.asyncio
async def test_error_handling(scraper):
    """Test error handling for invalid requests"""
    async with scraper:
        # Test with invalid event ID
        event = await scraper.get_event_details("invalid_id")
        assert event is None
        
        # Test with invalid location
        events = await scraper.scrape_events({"city": "NonexistentCity123"})
        assert len(events) == 0

@pytest.mark.asyncio
async def test_rate_limiting(scraper):
    """Test rate limiting behavior"""
    location = {
        "city": "San Francisco",
        "state": "CA",
        "country": "US"
    }
    
    async with scraper:
        # Make multiple requests in quick succession
        for _ in range(5):
            events = await scraper.scrape_events(location)
            assert len(events) > 0, "Rate limiting should not affect valid requests" 