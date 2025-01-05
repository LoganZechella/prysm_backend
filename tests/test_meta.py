import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from app.collectors.meta import MetaCollector
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

@pytest.fixture
def mock_meta_api():
    """Mock Meta (Facebook) API client."""
    with patch("facebook.GraphAPI") as mock:
        mock.return_value.get_object.return_value = {
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
        yield mock

@pytest.mark.asyncio
async def test_meta_collection(mock_meta_api):
    """Test collecting events from Meta (Facebook)."""
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
    assert "music" in event.categories
    assert event.source.url == "http://test.com/tickets" 