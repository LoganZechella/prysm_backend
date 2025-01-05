import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.event_collection import (
    EventbriteClient,
    transform_eventbrite_event,
    calculate_price_tier,
    calculate_popularity_score,
    extract_indoor_outdoor,
    extract_age_restriction,
    extract_accessibility_features,
    extract_dress_code
)

@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables for testing."""
    with patch.dict('os.environ', {'EVENTBRITE_API_KEY': 'mock_api_key_for_testing'}):
        yield

@pytest.fixture
def mock_eventbrite_response():
    """Mock response data from Eventbrite API."""
    return {
        'id': '123456789',
        'name': {'text': 'Test Event', 'html': '<p>Test Event</p>'},
        'description': {
            'text': 'This is an indoor event for ages 21+ with wheelchair access. Formal dress code required.',
            'html': '<p>This is an indoor event for ages 21+ with wheelchair access. Formal dress code required.</p>'
        },
        'url': 'https://eventbrite.com/e/123456789',
        'start': {'utc': '2024-03-01T19:00:00Z', 'timezone': 'America/Los_Angeles'},
        'end': {'utc': '2024-03-01T22:00:00Z', 'timezone': 'America/Los_Angeles'},
        'created': '2024-02-01T00:00:00Z',
        'venue': {
            'name': 'Test Venue',
            'address': {
                'localized_address_display': '123 Test St, San Francisco, CA 94105',
                'city': 'San Francisco',
                'region': 'CA',
                'country': 'US'
            },
            'latitude': '37.7749',
            'longitude': '-122.4194'
        },
        'ticket_availability': {
            'currency': 'USD'
        },
        'ticket_classes': [
            {'cost': {'major_value': 25.00}},
            {'cost': {'major_value': 50.00}}
        ],
        'categories': [
            {'name': 'Music'},
            {'name': 'Food & Drink'}
        ],
        'tags': [
            {'name': 'live-music'},
            {'name': 'food-tasting'}
        ],
        'images': [
            {'url': 'https://example.com/image1.jpg'},
            {'url': 'https://example.com/image2.jpg'}
        ],
        'organizer': {
            'verified': True
        },
        'capacity': 100,
        'tickets_sold': 75,
        'likes': 500,
        'shares': 200
    }

def test_eventbrite_client_initialization():
    """Test EventbriteClient initialization with missing API key."""
    with patch.dict('os.environ', clear=True):
        with pytest.raises(ValueError):
            EventbriteClient(api_key="")

def test_eventbrite_search_events():
    """Test EventbriteClient search_events method."""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {'events': []}
        mock_get.return_value.raise_for_status = Mock()
        
        client = EventbriteClient(api_key='test_key')
        result = client.search_events(
            location='San Francisco, CA',
            categories=['music'],
            start_date='2024-03-01T00:00:00Z',
            end_date='2024-03-31T23:59:59Z'
        )
        
        assert result == {'events': []}
        mock_get.assert_called_once()

def test_transform_eventbrite_event(mock_eventbrite_response):
    """Test event data transformation."""
    event = transform_eventbrite_event(mock_eventbrite_response)
    
    assert event.event_id == '123456789'
    assert event.title == 'Test Event'
    assert event.location.city == 'San Francisco'
    assert event.price_info.min_price == 25.00
    assert event.price_info.max_price == 50.00
    assert event.attributes.indoor_outdoor == 'indoor'
    assert event.attributes.age_restriction == 21
    assert 'wheelchair_accessible' in event.attributes.accessibility_features
    assert event.attributes.dress_code == 'formal'
    assert len(event.categories) == 2
    assert len(event.tags) == 2
    assert len(event.images) == 2
    assert event.metadata.verified is True

def test_calculate_price_tier():
    """Test price tier calculation."""
    assert calculate_price_tier(0.0) == 'free'
    assert calculate_price_tier(10.00) == 'low'
    assert calculate_price_tier(30.00) == 'medium'
    assert calculate_price_tier(80.00) == 'high'

def test_calculate_popularity_score(mock_eventbrite_response):
    """Test popularity score calculation."""
    score = calculate_popularity_score(mock_eventbrite_response)
    assert 0 <= score <= 1.0

def test_extract_indoor_outdoor():
    """Test indoor/outdoor extraction."""
    assert extract_indoor_outdoor("This is an indoor event") == 'indoor'
    assert extract_indoor_outdoor("This is an outdoor event") == 'outdoor'
    assert extract_indoor_outdoor("This event has both indoor and outdoor spaces") == 'unknown'
    assert extract_indoor_outdoor("This is an event") == 'unknown'

def test_extract_age_restriction():
    """Test age restriction extraction."""
    assert extract_age_restriction("This is a 21+ event") == 21
    assert extract_age_restriction("This is an 18+ event") == 18
    assert extract_age_restriction("This is an all ages event") is None

def test_extract_accessibility_features():
    """Test accessibility features extraction."""
    description = "This venue is wheelchair accessible with audio assistance and sign language interpretation"
    features = extract_accessibility_features(description)
    assert 'wheelchair_accessible' in features
    assert 'hearing_assistance' in features
    assert 'sign_language' in features

def test_extract_dress_code():
    """Test dress code extraction."""
    assert extract_dress_code("Black tie event") == 'formal'
    assert extract_dress_code("Business casual attire") == 'business casual'
    assert extract_dress_code("Come as you are") == 'casual' 