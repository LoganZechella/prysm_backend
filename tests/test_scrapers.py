import pytest
from datetime import datetime, timedelta
import json
from unittest.mock import patch, MagicMock
import aiohttp
from app.scrapers.base import BaseScraper
from app.scrapers.eventbrite import EventbriteScraper

class TestScraper(BaseScraper):
    """Test implementation of BaseScraper"""
    
    async def scrape_events(self, location, date_range=None):
        return []
    
    async def get_event_details(self, event_url):
        return {}

@pytest.fixture
def test_scraper():
    """Create a test scraper instance"""
    return TestScraper("test_platform")

@pytest.fixture
def eventbrite_scraper():
    """Create an Eventbrite scraper instance"""
    return EventbriteScraper("test_api_key")

@pytest.fixture
def mock_response():
    """Create a mock response"""
    class MockResponse:
        def __init__(self, status, text):
            self.status = status
            self._text = text
        
        async def text(self):
            return self._text
        
        async def __aexit__(self, exc_type, exc, tb):
            pass
        
        async def __aenter__(self):
            return self
    
    return MockResponse

@pytest.mark.asyncio
async def test_base_scraper_fetch_page(test_scraper, mock_response):
    """Test base scraper fetch_page method"""
    test_html = "<html><body>Test</body></html>"
    
    async with test_scraper:
        # Test successful fetch
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response(200, test_html)
            result = await test_scraper.fetch_page("http://test.com")
            assert result == test_html
        
        # Test rate limit handling
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.return_value = mock_response(429, "")
            result = await test_scraper.fetch_page("http://test.com")
            assert result is None
        
        # Test error handling
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_get.side_effect = aiohttp.ClientError()
            result = await test_scraper.fetch_page("http://test.com")
            assert result is None

@pytest.mark.asyncio
async def test_eventbrite_scrape_events(eventbrite_scraper, mock_response):
    """Test Eventbrite scraper events method"""
    # Mock event search response
    search_response = {
        "events": [
            {"id": "123", "name": {"text": "Test Event"}}
        ]
    }
    
    # Mock event details response
    event_details = {
        "id": "123",
        "name": {"text": "Test Event"},
        "description": {"text": "Test Description"},
        "start": {"utc": "2024-01-01T19:00:00Z"},
        "end": {"utc": "2024-01-01T22:00:00Z"},
        "url": "https://test.com/event",
        "venue": {
            "name": "Test Venue",
            "address": {
                "localized_address_display": "123 Test St",
                "city": "Test City",
                "region": "TC",
                "country": "US"
            },
            "latitude": "37.7749",
            "longitude": "-122.4194"
        },
        "category": {"name": "Music"},
        "ticket_classes": [
            {
                "cost": {"major_value": "25.00"},
                "free": False
            }
        ],
        "logo": {"original": {"url": "https://test.com/image.jpg"}}
    }
    
    location = {"lat": 37.7749, "lng": -122.4194}
    date_range = {
        "start": datetime.utcnow(),
        "end": datetime.utcnow() + timedelta(days=7)
    }
    
    async with eventbrite_scraper:
        with patch('app.scrapers.eventbrite.EventbriteScraper.fetch_page') as mock_fetch:
            # Mock search response
            mock_fetch.side_effect = [
                json.dumps(search_response),
                json.dumps(event_details)
            ]
            
            events = await eventbrite_scraper.scrape_events(location, date_range)
            
            assert len(events) == 1
            event = events[0]
            assert event["event_id"] == "123"
            assert event["title"] == "Test Event"
            assert event["location"]["venue_name"] == "Test Venue"
            assert event["price_info"]["price_tier"] == "medium"

@pytest.mark.asyncio
async def test_eventbrite_get_event_details(eventbrite_scraper, mock_response):
    """Test Eventbrite get_event_details method"""
    event_details = {
        "id": "123",
        "name": {"text": "Test Event"},
        "description": {"text": "Test Description"},
        "start": {"utc": "2024-01-01T19:00:00Z"},
        "end": {"utc": "2024-01-01T22:00:00Z"},
        "url": "https://test.com/event",
        "venue": {
            "name": "Test Venue",
            "address": {
                "localized_address_display": "123 Test St",
                "city": "Test City",
                "region": "TC",
                "country": "US"
            },
            "latitude": "37.7749",
            "longitude": "-122.4194"
        },
        "category": {"name": "Music"},
        "ticket_classes": [
            {
                "cost": {"major_value": "25.00"},
                "free": False
            }
        ],
        "logo": {"original": {"url": "https://test.com/image.jpg"}}
    }
    
    async with eventbrite_scraper:
        with patch('app.scrapers.eventbrite.EventbriteScraper.fetch_page') as mock_fetch:
            mock_fetch.return_value = json.dumps(event_details)
            
            result = await eventbrite_scraper.get_event_details("123")
            
            assert result["id"] == "123"
            assert result["title"] == "Test Event"
            assert result["venue"]["name"] == "Test Venue"
            assert result["price"]["tier"] == "medium"
            assert result["price"]["min"] == 25.0
            assert result["price"]["max"] == 25.0 