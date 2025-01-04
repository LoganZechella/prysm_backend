import pytest
import asyncio
from datetime import datetime, timedelta
from app.collectors.eventbrite import EventbriteCollector
from app.utils.storage import StorageManager
import os
from dotenv import load_dotenv
import logging
from app.utils.schema import Event, CategoryHierarchy

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@pytest.fixture
def mock_eventbrite_response():
    return {
        "id": "test123",
        "name": {"text": "Test Music Festival"},
        "description": {"text": "Join us for an amazing outdoor music festival! Live bands and great food."},
        "start": {"utc": "2024-01-01T19:00:00Z"},
        "end": {"utc": "2024-01-01T23:00:00Z"},
        "venue": {
            "name": "Test Venue",
            "address": {
                "address_1": "123 Test St",
                "city": "San Francisco",
                "region": "CA",
                "country": "US",
                "latitude": "37.7749",
                "longitude": "-122.4194"
            }
        },
        "categories": [
            {"name": "Music"},
            {"name": "Food & Drink"}
        ],
        "ticket_classes": [{
            "cost": {
                "currency": "USD",
                "actual": {"value": "45.00"}
            }
        }],
        "url": "https://test-event.com"
    }

@pytest.fixture
def collector():
    return EventbriteCollector("test_api_key")

def test_convert_to_event(collector, mock_eventbrite_response):
    """Test conversion of Eventbrite response to Event model with hierarchical categories"""
    event = collector._convert_to_event(mock_eventbrite_response)
    
    # Test basic event data
    assert event.event_id == "test123"
    assert event.title == "Test Music Festival"
    assert event.start_datetime.year == 2024
    assert event.location.city == "San Francisco"
    
    # Test raw categories
    assert "Music" in event.raw_categories
    assert "Food & Drink" in event.raw_categories
    
    # Test mapped categories
    assert "music" in event.categories
    assert "food_drink" in event.categories
    
    # Test category hierarchy
    assert "events" in event.get_all_categories()  # Root category
    assert "music" in event.get_all_categories()
    assert "food_drink" in event.get_all_categories()

@pytest.mark.asyncio
async def test_collect_events_with_categories(collector):
    """Test collecting events with category filtering"""
    # Mock the API responses
    async def mock_get(*args, **kwargs):
        if "categories" in args[0]:
            return MockResponse(200, {
                "categories": [
                    {"id": "103", "name": "Music"},
                    {"id": "110", "name": "Food & Drink"}
                ]
            })
        else:
            return MockResponse(200, {
                "events": [mock_eventbrite_response()]
            })
    
    collector._client = MockClient(mock_get)
    
    # Test with our hierarchical categories
    events = await collector.collect_events(
        categories=["music", "food_drink"]
    )
    
    assert len(events) == 1
    event = events[0]
    
    # Verify category mapping
    assert "music" in event.categories
    assert "food_drink" in event.categories
    assert "events" in event.get_all_categories()

class MockResponse:
    def __init__(self, status_code, json_data):
        self.status_code = status_code
        self._json_data = json_data
    
    def json(self):
        return self._json_data

class MockClient:
    def __init__(self, mock_get):
        self.mock_get = mock_get
    
    async def get(self, *args, **kwargs):
        return await self.mock_get(*args, **kwargs)

if __name__ == "__main__":
    print("Testing Eventbrite collector...")
    print("=" * 50)
    pytest.main([__file__]) 