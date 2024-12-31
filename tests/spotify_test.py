import pytest
from unittest.mock import Mock, patch
from utils.spotify_utils import get_spotify_client, fetch_trending_tracks, upload_to_gcs

@pytest.fixture
def mock_spotify_client():
    return Mock()

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client

def test_get_spotify_client():
    with patch('spotipy.Spotify') as mock_spotify:
        with patch('spotipy.oauth2.SpotifyClientCredentials') as mock_credentials:
            client = get_spotify_client()
            assert mock_credentials.called
            assert mock_spotify.called

def test_fetch_trending_tracks(mock_spotify_client):
    # Mock the featured playlists response
    mock_spotify_client.featured_playlists.return_value = {
        'playlists': {
            'items': [{'id': 'playlist1'}, {'id': 'playlist2'}]
        }
    }
    
    # Mock the playlist tracks response
    mock_spotify_client.playlist_tracks.return_value = {
        'items': [
            {'track': {'id': 'track1', 'name': 'Track 1'}},
            {'track': {'id': 'track2', 'name': 'Track 2'}}
        ]
    }
    
    tracks = fetch_trending_tracks(mock_spotify_client, limit=10)
    
    assert len(tracks) > 0
    assert all(isinstance(track, dict) for track in tracks)
    assert mock_spotify_client.featured_playlists.called
    assert mock_spotify_client.playlist_tracks.called

def test_upload_to_gcs(mock_storage_client):
    test_data = {'test': 'data'}
    bucket_name = 'test-bucket'
    blob_path = 'test/path.json'
    
    # Mock the bucket and blob
    mock_bucket = Mock()
    mock_blob = Mock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    result = upload_to_gcs(test_data, bucket_name, blob_path)
    
    assert result == f'gs://{bucket_name}/{blob_path}'
    assert mock_storage_client.called
    assert mock_bucket.blob.called
    assert mock_blob.upload_from_string.called 