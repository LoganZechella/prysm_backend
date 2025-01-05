import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import pytest_asyncio
from app.collectors.eventbrite import EventbriteCollector
from app.collectors.meta import MetaCollector
from app.collectors.scraper import EventScraper
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

@pytest_asyncio.fixture
async def mock_eventbrite():
    """Mock Eventbrite API client."""
    with patch("app.collectors.eventbrite.Eventbrite") as mock:
        yield mock

@pytest_asyncio.fixture
async def mock_meta():
    """Mock Meta (Facebook) API client."""
    with patch("app.collectors.meta.GraphAPI") as mock:
        yield mock

@pytest_asyncio.fixture
async def mock_scrapfly():
    """Mock Scrapfly API client."""
    with patch("app.collectors.scraper.ScrapflyClient") as mock:
        yield mock

@pytest.mark.asyncio
async def test_eventbrite_collection(mock_eventbrite):
    """Test collecting events from Eventbrite."""
    # Mock API response
    mock_response = {
        "events": [{
            "id": "test-event-1",
            "name": {"text": "Test Event"},
            "description": {"text": "Test Description"},
            "start": {"utc": "2024-03-01T19:00:00Z"},
            "end": {"utc": "2024-03-01T22:00:00Z"},
            "venue": {
                "name": "Test Venue",
                "address": {
                    "city": "Test City",
                    "latitude": 37.7749,
                    "longitude": -122.4194
                }
            },
            "category": {"name": "Music"},
            "ticket_availability": {
                "minimum_ticket_price": {"value": 10.00, "currency": "USD"},
                "maximum_ticket_price": {"value": 50.00, "currency": "USD"}
            }
        }]
    }
    mock_eventbrite.return_value.get.return_value = mock_response
    
    collector = EventbriteCollector("test-token")
    events = await collector.collect_events()
    
    assert len(events) == 1
    event = events[0]
    assert event.event_id == "test-event-1"
    assert event.title == "Test Event"
    assert event.description == "Test Description"
    assert event.location.venue_name == "Test Venue"
    assert event.location.city == "Test City"
    assert event.location.coordinates["lat"] == 37.7749
    assert event.location.coordinates["lon"] == -122.4194
    assert "Music" in event.categories
    assert event.price_info is not None
    assert event.price_info.min_price == 10.00
    assert event.price_info.max_price == 50.00
    assert event.price_info.currency == "USD"

@pytest.mark.asyncio
async def test_meta_collection(mock_meta):
    """Test collecting events from Meta (Facebook)."""
    # Mock API response
    mock_response = {
        "data": [{
            "id": "test-event-1",
            "name": "Test Event",
            "description": "Test Description",
            "start_time": "2024-03-01T19:00:00+0000",
            "end_time": "2024-03-01T22:00:00+0000",
            "place": {
                "name": "Test Venue",
                "location": {
                    "city": "Test City",
                    "latitude": 37.7749,
                    "longitude": -122.4194
                }
            },
            "category": "MUSIC_EVENT",
            "ticket_uri": "http://test.com/tickets"
        }]
    }
    mock_meta.return_value.get_object.return_value = mock_response
    
    collector = MetaCollector("test-token")
    events = await collector.collect_events()
    
    assert len(events) == 1
    event = events[0]
    assert event.event_id == "test-event-1"
    assert event.title == "Test Event"
    assert event.description == "Test Description"
    assert event.location.venue_name == "Test Venue"
    assert event.location.city == "Test City"
    assert event.location.coordinates["lat"] == 37.7749
    assert event.location.coordinates["lon"] == -122.4194
    assert "Music" in event.categories
    assert event.ticket_url == "http://test.com/tickets"

def test_scraper_parse_eventbrite(mock_scrapfly):
    """Test parsing Eventbrite HTML."""
    # Mock HTML response
    mock_response = {
        "content": """
            <div class="event-card">
                <h3 class="Typography_body-lg__487rx">Test Event</h3>
                <p class="Typography_body-md__487rx">Test Description</p>
                <p class="Typography_body-md-bold__487rx">Tomorrow at 7:00 PM</p>
                <a class="event-card-link" data-event-location="Test City">Test Venue</a>
                <p style="color: #716b7a">$10-$50</p>
            </div>
        """
    }
    
    scraper = EventScraper()
    events = scraper._parse_eventbrite(mock_response)
    
    assert len(events) == 1
    event = events[0]
    assert event.title == "Test Event"
    assert event.description == "Test Description"
    assert event.location.venue_name == "Test Venue"
    assert event.location.city == "Test City"
    assert event.price_info is not None
    assert event.price_info.min_price == 10.00
    assert event.price_info.max_price == 50.00
    assert event.price_info.currency == "USD" 