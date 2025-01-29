import pytest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from app.utils.data_processing_utils import (
    read_from_gcs,
    process_spotify_data,
    process_trends_data,
    process_linkedin_data,
    upload_processed_data
)

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client

@pytest.fixture
def sample_spotify_data():
    tracks_data = [
        {
            'id': 'track1',
            'name': 'Test Track 1',
            'popularity': 80,
            'artists': [{'id': 'artist1', 'name': 'Test Artist 1'}],
            'album': {'name': 'Test Album 1', 'release_date': '2024-01-01'},
            'duration_ms': 180000,
            'explicit': False
        }
    ]
    
    artists_data = [
        {
            'id': 'artist1',
            'name': 'Test Artist 1',
            'genres': ['pop', 'rock'],
            'popularity': 85,
            'followers': {'total': 1000000}
        }
    ]
    
    return tracks_data, artists_data

@pytest.fixture
def sample_trends_data():
    topics_data = [
        {
            'topic': 'Test Topic 1',
            'interest_over_time': {'2024-01-01': 100},
            'timestamp': '2024-01-01T00:00:00'
        }
    ]
    
    interest_data = {
        'Test Topic 1': {'2024-01-01': 100}
    }
    
    related_data = {
        'Test Topic 1': ['Related Topic 1', 'Related Topic 2']
    }
    
    return topics_data, interest_data, related_data

@pytest.fixture
def sample_linkedin_data():
    profile_data = {
        'id': 'user1',
        'firstName': {'localized': {'en_US': 'Test'}},
        'lastName': {'localized': {'en_US': 'User'}},
        'headline': {'localized': {'en_US': 'Test Headline'}},
        'industry': {'localized': {'en_US': 'Technology'}}
    }
    
    connections_data = {
        'elements': [
            {
                'id': 'connection1',
                'firstName': {'localized': {'en_US': 'Connection'}},
                'lastName': {'localized': {'en_US': 'One'}},
                'headline': {'localized': {'en_US': 'Connection Headline'}},
                'industry': {'localized': {'en_US': 'Technology'}}
            }
        ]
    }
    
    activity_data = {
        'elements': [
            {
                'id': 'activity1',
                'type': 'POST',
                'created': {'time': '2024-01-01T00:00:00'},
                'text': {'text': 'Test post'}
            }
        ]
    }
    
    return profile_data, connections_data, activity_data

def test_read_from_gcs(mock_storage_client):
    test_data = {'test': 'data'}
    mock_blob = Mock()
    mock_blob.download_as_string.return_value = json.dumps(test_data).encode()
    mock_storage_client.return_value.bucket.return_value.blob.return_value = mock_blob
    
    result = read_from_gcs('test-bucket', 'test/path.json')
    
    assert result == test_data
    assert mock_storage_client.called
    assert mock_blob.download_as_string.called

def test_process_spotify_data(sample_spotify_data):
    tracks_data, artists_data = sample_spotify_data
    processed_tracks, processed_artists = process_spotify_data(tracks_data, artists_data)
    
    assert len(processed_tracks) == 1
    assert len(processed_artists) == 1
    
    track = processed_tracks[0]
    assert track['id'] == 'track1'
    assert track['name'] == 'Test Track 1'
    assert track['artist_id'] == 'artist1'
    assert track['artist_name'] == 'Test Artist 1'
    
    artist = processed_artists[0]
    assert artist['id'] == 'artist1'
    assert artist['name'] == 'Test Artist 1'
    assert 'pop' in artist['genres']
    assert artist['followers'] == 1000000

def test_process_trends_data(sample_trends_data):
    topics_data, interest_data, related_data = sample_trends_data
    processed_topics, processed_interest, processed_related = process_trends_data(
        topics_data, interest_data, related_data
    )
    
    assert len(processed_topics) == 1
    assert processed_topics[0]['topic'] == 'Test Topic 1'
    assert processed_interest['data'] == interest_data
    assert processed_related['data'] == related_data

def test_process_linkedin_data(sample_linkedin_data):
    profile_data, connections_data, activity_data = sample_linkedin_data
    processed_profile, processed_connections, processed_activities = process_linkedin_data(
        profile_data, connections_data, activity_data
    )
    
    assert processed_profile['id'] == 'user1'
    assert processed_profile['first_name'] == 'Test'
    assert processed_profile['last_name'] == 'User'
    
    assert len(processed_connections) == 1
    connection = processed_connections[0]
    assert connection['id'] == 'connection1'
    assert connection['first_name'] == 'Connection'
    
    assert len(processed_activities) == 1
    activity = processed_activities[0]
    assert activity['id'] == 'activity1'
    assert activity['type'] == 'POST'

def test_upload_processed_data(mock_storage_client):
    test_data = {'test': 'data'}
    bucket_name = 'test-bucket'
    blob_path = 'test/path.json'
    
    mock_bucket = Mock()
    mock_blob = Mock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    result = upload_processed_data(test_data, bucket_name, blob_path)
    
    assert result == f'gs://{bucket_name}/{blob_path}'
    assert mock_storage_client.called
    assert mock_bucket.blob.called
    assert mock_blob.upload_from_string.called 