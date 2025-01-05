import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from typing import Dict, Any, List

@pytest.fixture
def mock_aws():
    """Mock AWS services."""
    with patch("boto3.client") as mock_client:
        yield mock_client

@pytest.fixture
def sample_event_data() -> Dict[str, Any]:
    """Sample event data for testing."""
    return {
        "id": "test-event-1",
        "title": "Test Event",
        "description": "A test event for unit testing",
        "start_time": "2024-01-01T10:00:00",
        "end_time": "2024-01-01T12:00:00",
        "location": {
            "venue": "Test Venue",
            "address": "123 Test St",
            "city": "Test City",
            "state": "TS",
            "country": "Test Country",
            "coordinates": {"lat": 37.7749, "lon": -122.4194}
        },
        "categories": ["test", "event"],
        "price_info": {
            "min_price": 10.0,
            "max_price": 20.0,
            "currency": "USD"
        },
        "source_info": {
            "platform": "test",
            "url": "https://test.com/event-1"
        }
    }

@pytest.fixture
def sample_trends_data() -> pd.DataFrame:
    """Sample trends data for testing."""
    return pd.DataFrame({
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "category": ["music", "sports", "arts"],
        "searches": [100, 200, 150]
    })

@pytest.fixture
def mock_storage_manager():
    """Mock storage manager for testing."""
    mock = MagicMock()
    mock.get_events.return_value = []
    mock.get_user_preferences.return_value = None
    mock.get_cache.return_value = None
    return mock 