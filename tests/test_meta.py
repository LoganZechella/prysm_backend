import pytest
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from app.collectors.meta import MetaCollector
from app.utils.storage import StorageManager
import httpx
from app.utils.schema import Event, CategoryHierarchy

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Testing Meta collector...")
print("=" * 50)

@pytest.fixture
def mock_meta_response():
    return {
        "id": "test123",
        "name": "Art Gallery Opening",
        "description": "Join us for an art gallery opening featuring local artists and live music performances.",
        "start_time": "2024-01-01T19:00:00Z",
        "end_time": "2024-01-01T23:00:00Z",
        "place": {
            "name": "Test Gallery",
            "location": {
                "street": "123 Test St",
                "city": "San Francisco",
                "state": "CA",
                "country": "US",
                "latitude": 37.7749,
                "longitude": -122.4194
            }
        },
        "category": "ART_EVENT",
        "event_type": "ARTS_ENTERTAINMENT",
        "ticket_info": {
            "currency": "USD",
            "min_price": 25.00,
            "max_price": 45.00
        },
        "event_url": "https://test-event.com"
    }

@pytest.fixture
def collector():
    return MetaCollector("test_access_token")

def test_convert_to_event(collector, mock_meta_response):
    """Test conversion of Meta response to Event model with hierarchical categories"""
    event = collector._convert_to_event(mock_meta_response)
    
    # Test basic event data
    assert event.event_id == "test123"
    assert event.title == "Art Gallery Opening"
    assert event.start_datetime.year == 2024
    assert event.location.city == "San Francisco"
    
    # Test raw categories
    assert "ART_EVENT" in event.raw_categories
    assert "ARTS_ENTERTAINMENT" in event.raw_categories
    
    # Test mapped categories
    assert "visual_arts" in event.categories
    assert "arts_culture" in event.categories
    
    # Test category hierarchy
    assert "events" in event.get_all_categories()  # Root category
    assert "arts_culture" in event.get_all_categories()
    assert "visual_arts" in event.get_all_categories()

@pytest.mark.asyncio
async def test_collect_events_with_categories(collector):
    """Test collecting events with category filtering"""
    # Mock the API responses
    async def mock_get(*args, **kwargs):
        if "event_categories" in args[0]:
            return MockResponse(200, {
                "data": [
                    {"id": "ART_EVENT", "name": "Art Event"},
                    {"id": "MUSIC_EVENT", "name": "Music Event"}
                ]
            })
        else:
            return MockResponse(200, {
                "data": [mock_meta_response()]
            })
    
    collector._client = MockClient(mock_get)
    
    # Test with our hierarchical categories
    events = await collector.collect_events(
        categories=["visual_arts", "arts_culture"]
    )
    
    assert len(events) == 1
    event = events[0]
    
    # Verify category mapping
    assert "visual_arts" in event.categories
    assert "arts_culture" in event.categories
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
    pytest.main([__file__]) 