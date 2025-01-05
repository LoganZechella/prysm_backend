import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from app.collectors.eventbrite import EventbriteCollector
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

@pytest.fixture
def mock_eventbrite_api():
    """Mock Eventbrite API client."""
    with patch("eventbrite.Eventbrite") as mock:
        mock.return_value.get.return_value = {
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
        yield mock

@pytest.mark.asyncio
async def test_eventbrite_collection(mock_eventbrite_api):
    """Test collecting events from Eventbrite."""
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
    assert "music" in event.categories
    assert event.price_info is not None
    assert event.price_info.min_price == 10.00
    assert event.price_info.max_price == 50.00
    assert event.price_info.currency == "USD" 