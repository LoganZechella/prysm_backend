import pytest
from datetime import datetime, timedelta
from utils.data_quality_utils import (
    validate_spotify_data,
    validate_trends_data,
    validate_event_data,
    validate_processed_data,
    check_data_freshness,
    check_data_completeness
)

@pytest.fixture
def mock_spotify_data():
    """Mock Spotify data for testing."""
    tracks_data = [
        {
            'id': '1',
            'name': 'Test Track',
            'artists': [{'id': '1', 'name': 'Test Artist'}]
        }
    ]
    artists_data = [
        {
            'id': '1',
            'name': 'Test Artist'
        }
    ]
    return tracks_data, artists_data

@pytest.fixture
def mock_trends_data():
    """Mock Google Trends data for testing."""
    topics_data = [
        {
            'topic': 'Test Topic',
            'interest_over_time': {'2024-01': 100}
        }
    ]
    interest_data = {
        'Test Topic': {'2024-01': 100}
    }
    related_data = {
        'Test Topic': ['Related Topic 1', 'Related Topic 2']
    }
    return topics_data, interest_data, related_data

@pytest.fixture
def mock_event_data():
    """Mock event data for testing."""
    return [
        {
            'event_id': '1',
            'title': 'Test Event',
            'start_datetime': '2024-03-01T19:00:00Z',
            'location': {
                'venue_name': 'Test Venue'
            }
        }
    ]

@pytest.fixture
def mock_processed_data():
    """Mock processed data for testing."""
    return {
        'id': '1',
        'name': 'Test',
        'count': 42,
        'active': True,
        'timestamp': '2024-03-01T00:00:00Z'
    }

def test_validate_spotify_data_valid(mock_spotify_data):
    """Test validation of valid Spotify data."""
    tracks_data, artists_data = mock_spotify_data
    is_valid, errors = validate_spotify_data(tracks_data, artists_data)
    assert is_valid
    assert not errors

def test_validate_spotify_data_invalid():
    """Test validation of invalid Spotify data."""
    tracks_data = [{'name': 'Missing ID'}]
    artists_data = [{'id': 'Missing Name'}]
    is_valid, errors = validate_spotify_data(tracks_data, artists_data)
    assert not is_valid
    assert len(errors) == 3  # Missing ID, missing artists, and missing name
    assert any('missing ID' in error.lower() for error in errors)
    assert any('missing artists' in error.lower() for error in errors)
    assert any('missing name' in error.lower() for error in errors)

def test_validate_trends_data_valid(mock_trends_data):
    """Test validation of valid Google Trends data."""
    topics_data, interest_data, related_data = mock_trends_data
    is_valid, errors = validate_trends_data(topics_data, interest_data, related_data)
    assert is_valid
    assert not errors

def test_validate_trends_data_invalid():
    """Test validation of invalid Google Trends data."""
    topics_data = [{'interest_over_time': {}}]  # Missing topic name
    interest_data = {'Unknown Topic': {}}  # Topic not in topics_data
    related_data = {}  # Empty related data
    is_valid, errors = validate_trends_data(topics_data, interest_data, related_data)
    assert not is_valid
    assert len(errors) >= 2

def test_validate_event_data_valid(mock_event_data):
    """Test validation of valid event data."""
    is_valid, errors = validate_event_data(mock_event_data)
    assert is_valid
    assert not errors

def test_validate_event_data_invalid():
    """Test validation of invalid event data."""
    events_data = [
        {
            'title': 'Missing ID',
            'start_datetime': '2024-03-01T19:00:00Z'
        }
    ]
    is_valid, errors = validate_event_data(events_data)
    assert not is_valid
    assert len(errors) >= 1

def test_validate_processed_data_valid(mock_processed_data):
    """Test validation of valid processed data."""
    schema = {
        'id': str,
        'name': str,
        'count': int,
        'active': bool,
        'timestamp': str
    }
    required_fields = ['id', 'name']
    is_valid, errors = validate_processed_data(mock_processed_data, schema, required_fields)
    assert is_valid
    assert not errors

def test_validate_processed_data_invalid():
    """Test validation of invalid processed data."""
    data = {
        'id': 123,  # Wrong type (int instead of str)
        'active': 'yes'  # Wrong type (str instead of bool)
    }
    schema = {
        'id': str,
        'name': str,
        'active': bool
    }
    required_fields = ['id', 'name']
    is_valid, errors = validate_processed_data(data, schema, required_fields)
    assert not is_valid
    assert len(errors) >= 2

def test_check_data_freshness_valid():
    """Test freshness check with valid timestamp."""
    timestamp = datetime.utcnow().isoformat() + 'Z'
    is_fresh, error = check_data_freshness(timestamp)
    assert is_fresh
    assert not error

def test_check_data_freshness_invalid():
    """Test freshness check with old timestamp."""
    old_timestamp = (datetime.utcnow() - timedelta(hours=25)).isoformat() + 'Z'
    is_fresh, error = check_data_freshness(old_timestamp)
    assert not is_fresh
    assert error

def test_check_data_completeness_valid():
    """Test completeness check with all required fields."""
    data = {'id': '1', 'name': 'Test', 'optional': 'value'}
    required_fields = ['id', 'name']
    is_complete, missing = check_data_completeness(data, required_fields)
    assert is_complete
    assert not missing

def test_check_data_completeness_invalid():
    """Test completeness check with missing fields."""
    data = {'id': '1', 'optional': 'value'}
    required_fields = ['id', 'name']
    is_complete, missing = check_data_completeness(data, required_fields)
    assert not is_complete
    assert 'name' in missing 