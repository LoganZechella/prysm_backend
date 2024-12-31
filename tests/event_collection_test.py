import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.event_collection_utils import (
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
            EventbriteClient()

def test_eventbrite_search_events():
    """Test EventbriteClient search_events method."""
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {'events': []}
        mock_get.return_value.raise_for_status = Mock()
        
        client = EventbriteClient()
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
    transformed = transform_eventbrite_event(mock_eventbrite_response)
    
    assert transformed['event_id'] == '123456789'
    assert transformed['title'] == 'Test Event'
    assert transformed['location']['city'] == 'San Francisco'
    assert transformed['price_info']['min_price'] == 25.00
    assert transformed['price_info']['max_price'] == 50.00
    assert transformed['attributes']['indoor_outdoor'] == 'indoor'
    assert transformed['attributes']['age_restriction'] == '21+'
    assert 'wheelchair' in transformed['attributes']['accessibility_features']
    assert transformed['attributes']['dress_code'] == 'formal'
    assert len(transformed['categories']) == 2
    assert len(transformed['tags']) == 2
    assert len(transformed['images']) == 2
    assert transformed['metadata']['verified'] is True

def test_calculate_price_tier():
    """Test price tier calculation."""
    assert calculate_price_tier(None, None) == 'free'
    assert calculate_price_tier(10.00, 20.00) == 'budget'
    assert calculate_price_tier(30.00, 70.00) == 'medium'
    assert calculate_price_tier(80.00, 120.00) == 'premium'

def test_calculate_popularity_score(mock_eventbrite_response):
    """Test popularity score calculation."""
    score = calculate_popularity_score(mock_eventbrite_response)
    assert 0 <= score <= 1.0

def test_extract_indoor_outdoor():
    """Test indoor/outdoor extraction."""
    indoor_event = {'description': {'text': 'This is an indoor event'}}
    outdoor_event = {'description': {'text': 'This is an outdoor event'}}
    both_event = {'description': {'text': 'This event has both indoor and outdoor spaces'}}
    unspecified_event = {'description': {'text': 'This is an event'}}
    
    assert extract_indoor_outdoor(indoor_event) == 'indoor'
    assert extract_indoor_outdoor(outdoor_event) == 'outdoor'
    assert extract_indoor_outdoor(both_event) == 'both'
    assert extract_indoor_outdoor(unspecified_event) == 'indoor'

def test_extract_age_restriction():
    """Test age restriction extraction."""
    event_21_plus = {'description': {'text': 'This is a 21+ event'}}
    event_18_plus = {'description': {'text': 'This is an 18+ event'}}
    event_all_ages = {'description': {'text': 'This is an all ages event'}}
    
    assert extract_age_restriction(event_21_plus) == '21+'
    assert extract_age_restriction(event_18_plus) == '18+'
    assert extract_age_restriction(event_all_ages) == 'all'

def test_extract_accessibility_features():
    """Test accessibility features extraction."""
    event = {
        'description': {
            'text': 'This venue is wheelchair accessible with audio assistance and sign language interpretation'
        }
    }
    
    features = extract_accessibility_features(event)
    assert 'wheelchair' in features
    assert 'hearing_assistance' in features
    assert 'sign_language' in features

def test_extract_dress_code():
    """Test dress code extraction."""
    formal_event = {'description': {'text': 'Black tie event'}}
    business_event = {'description': {'text': 'Business casual attire'}}
    casual_event = {'description': {'text': 'Come as you are'}}
    
    assert extract_dress_code(formal_event) == 'formal'
    assert extract_dress_code(business_event) == 'business'
    assert extract_dress_code(casual_event) == 'casual' 