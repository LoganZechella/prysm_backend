import pytest
from datetime import datetime, timedelta
from utils.data_quality_utils import (
    validate_spotify_data,
    validate_trends_data,
    validate_linkedin_data,
    check_data_freshness,
    check_data_completeness,
    validate_processed_data
)

@pytest.fixture
def valid_spotify_data():
    tracks_data = [
        {
            'id': 'track1',
            'name': 'Test Track',
            'artists': [{'id': 'artist1', 'name': 'Test Artist'}]
        }
    ]
    artists_data = [
        {
            'id': 'artist1',
            'name': 'Test Artist'
        }
    ]
    return tracks_data, artists_data

@pytest.fixture
def invalid_spotify_data():
    tracks_data = [
        {
            'id': 'track1',
            # Missing name
            'artists': [{'id': 'artist1', 'name': 'Test Artist'}]
        }
    ]
    artists_data = [
        {
            'id': 'artist1',
            # Missing name
        }
    ]
    return tracks_data, artists_data

@pytest.fixture
def valid_trends_data():
    topics_data = [
        {
            'topic': 'Test Topic',
            'interest_over_time': {'2024-01-01': 100}
        }
    ]
    interest_data = {
        'Test Topic': {'2024-01-01': 100}
    }
    related_data = {
        'Test Topic': ['Related Topic 1', 'Related Topic 2']
    }
    return topics_data, interest_data, related_data

@pytest.fixture
def invalid_trends_data():
    topics_data = [
        {
            # Missing topic name
            'interest_over_time': {'2024-01-01': 100}
        }
    ]
    interest_data = {
        'Unknown Topic': {'2024-01-01': 100}  # Topic not in topics_data
    }
    related_data = {}  # Empty related data
    return topics_data, interest_data, related_data

@pytest.fixture
def valid_linkedin_data():
    profile_data = {
        'id': 'user1',
        'firstName': {'localized': {'en_US': 'Test'}},
        'lastName': {'localized': {'en_US': 'User'}}
    }
    connections_data = {
        'elements': [
            {
                'id': 'connection1',
                'firstName': {'localized': {'en_US': 'Connection'}},
                'lastName': {'localized': {'en_US': 'One'}}
            }
        ]
    }
    activity_data = {
        'elements': [
            {
                'id': 'activity1',
                'type': 'POST'
            }
        ]
    }
    return profile_data, connections_data, activity_data

@pytest.fixture
def invalid_linkedin_data():
    profile_data = {
        # Missing id
        'firstName': {'localized': {'en_US': 'Test'}},
        # Missing lastName
    }
    connections_data = {
        'elements': [
            {
                # Missing id
                'firstName': {'localized': {'en_US': 'Connection'}},
                # Missing lastName
            }
        ]
    }
    activity_data = {
        'elements': [
            {
                'id': 'activity1',
                # Missing type
            }
        ]
    }
    return profile_data, connections_data, activity_data

def test_validate_spotify_data_valid(valid_spotify_data):
    tracks_data, artists_data = valid_spotify_data
    is_valid, errors = validate_spotify_data(tracks_data, artists_data)
    assert is_valid
    assert not errors

def test_validate_spotify_data_invalid(invalid_spotify_data):
    tracks_data, artists_data = invalid_spotify_data
    is_valid, errors = validate_spotify_data(tracks_data, artists_data)
    assert not is_valid
    assert len(errors) > 0
    assert any('missing name' in error.lower() for error in errors)

def test_validate_trends_data_valid(valid_trends_data):
    topics_data, interest_data, related_data = valid_trends_data
    is_valid, errors = validate_trends_data(topics_data, interest_data, related_data)
    assert is_valid
    assert not errors

def test_validate_trends_data_invalid(invalid_trends_data):
    topics_data, interest_data, related_data = invalid_trends_data
    is_valid, errors = validate_trends_data(topics_data, interest_data, related_data)
    assert not is_valid
    assert len(errors) > 0
    assert any('missing name' in error.lower() for error in errors)
    assert any('unknown topic' in error.lower() for error in errors)

def test_validate_linkedin_data_valid(valid_linkedin_data):
    profile_data, connections_data, activity_data = valid_linkedin_data
    is_valid, errors = validate_linkedin_data(profile_data, connections_data, activity_data)
    assert is_valid
    assert not errors

def test_validate_linkedin_data_invalid(invalid_linkedin_data):
    profile_data, connections_data, activity_data = invalid_linkedin_data
    is_valid, errors = validate_linkedin_data(profile_data, connections_data, activity_data)
    assert not is_valid
    assert len(errors) > 0
    assert any('missing id' in error.lower() for error in errors)

def test_check_data_freshness():
    # Test fresh data
    fresh_timestamp = datetime.now().isoformat()
    is_fresh, error = check_data_freshness(fresh_timestamp, max_age_hours=24)
    assert is_fresh
    assert error is None
    
    # Test stale data
    stale_timestamp = (datetime.now() - timedelta(hours=48)).isoformat()
    is_fresh, error = check_data_freshness(stale_timestamp, max_age_hours=24)
    assert not is_fresh
    assert error is not None
    assert 'too old' in error.lower()
    
    # Test invalid timestamp
    is_fresh, error = check_data_freshness('invalid-timestamp')
    assert not is_fresh
    assert error is not None
    assert 'invalid' in error.lower()

def test_check_data_completeness():
    # Test complete data
    data = {'field1': 'value1', 'field2': 'value2'}
    required_fields = ['field1', 'field2']
    is_complete, missing = check_data_completeness(data, required_fields)
    assert is_complete
    assert not missing
    
    # Test incomplete data
    data = {'field1': 'value1'}
    is_complete, missing = check_data_completeness(data, required_fields)
    assert not is_complete
    assert 'field2' in missing
    
    # Test null values
    data = {'field1': 'value1', 'field2': None}
    is_complete, missing = check_data_completeness(data, required_fields)
    assert not is_complete
    assert 'field2' in missing

def test_validate_processed_data():
    # Test valid data
    data = {
        'string_field': 'test',
        'int_field': 123,
        'list_field': ['item1', 'item2']
    }
    schema = {
        'string_field': str,
        'int_field': int,
        'list_field': list
    }
    required_fields = ['string_field', 'int_field']
    
    is_valid, errors = validate_processed_data(data, schema, required_fields)
    assert is_valid
    assert not errors
    
    # Test invalid types
    invalid_data = {
        'string_field': 123,  # Should be string
        'int_field': '123',   # Should be int
        'list_field': 'not_a_list'  # Should be list
    }
    
    is_valid, errors = validate_processed_data(invalid_data, schema, required_fields)
    assert not is_valid
    assert len(errors) > 0
    assert any('invalid type' in error.lower() for error in errors)
    
    # Test missing required fields
    incomplete_data = {
        'string_field': 'test'
        # Missing int_field
    }
    
    is_valid, errors = validate_processed_data(incomplete_data, schema, required_fields)
    assert not is_valid
    assert len(errors) > 0
    assert any('missing required field' in error.lower() for error in errors) 