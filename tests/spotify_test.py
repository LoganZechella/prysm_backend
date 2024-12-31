import pytest
from unittest.mock import Mock, patch
from utils.spotify_utils import (
    get_spotify_client,
    fetch_trending_tracks,
    upload_to_gcs,
    fetch_spotify_data
)

@pytest.fixture
def mock_spotify_client():
    """Mock Spotify client for testing."""
    client = Mock()
    client.featured_playlists.return_value = {
        'playlists': {
            'items': [
                {'id': 'playlist1'},
                {'id': 'playlist2'}
            ]
        }
    }
    client.playlist_tracks.return_value = {
        'items': [
            {'track': {'id': '1', 'name': 'Track 1', 'artists': [{'id': 'artist1', 'name': 'Artist 1'}]}},
            {'track': {'id': '2', 'name': 'Track 2', 'artists': [{'id': 'artist2', 'name': 'Artist 2'}]}}
        ]
    }
    client.artist.return_value = {
        'id': 'artist1',
        'name': 'Artist 1',
        'genres': ['pop'],
        'popularity': 80
    }
    return client

@pytest.fixture
def mock_storage_client():
    """Mock Google Cloud Storage client for testing."""
    with patch('google.cloud.storage.Client') as mock_client:
        bucket = Mock()
        blob = Mock()
        bucket.blob.return_value = blob
        mock_client.return_value.bucket.return_value = bucket
        yield mock_client

def test_get_spotify_client():
    """Test Spotify client initialization."""
    with patch('spotipy.Spotify') as mock_spotify:
        with patch.dict('os.environ', {
            'SPOTIFY_CLIENT_ID': 'test_id',
            'SPOTIFY_CLIENT_SECRET': 'test_secret'
        }):
            client = get_spotify_client()
            assert client is not None
            mock_spotify.assert_called_once()

def test_fetch_trending_tracks(mock_spotify_client):
    """Test fetching trending tracks."""
    tracks = fetch_trending_tracks(mock_spotify_client)
    assert len(tracks) == 4  # 2 playlists * 2 tracks each
    assert all('id' in track for track in tracks)
    assert all('name' in track for track in tracks)
    assert all('artists' in track for track in tracks)

def test_upload_to_gcs(mock_storage_client):
    """Test uploading data to GCS."""
    data = {'test': 'data'}
    bucket_name = 'test-bucket'
    blob_path = 'test/path.json'
    
    result = upload_to_gcs(data, bucket_name, blob_path)
    
    assert result == f'gs://{bucket_name}/{blob_path}'
    mock_storage_client.assert_called_once()
    bucket = mock_storage_client.return_value.bucket.return_value
    blob = bucket.blob.return_value
    blob.upload_from_string.assert_called_once()

def test_fetch_spotify_data(mock_spotify_client, mock_storage_client):
    """Test the main Spotify data fetching function."""
    with patch('utils.spotify_utils.get_spotify_client', return_value=mock_spotify_client):
        context = {'task_instance': Mock()}
        result = fetch_spotify_data(**context)
        
        assert result.startswith('gs://prysm-raw-data/spotify/raw/tracks_')
        assert result.endswith('.json')
        
        # Verify XCom pushes
        context['task_instance'].xcom_push.assert_any_call(
            key='tracks_path',
            value=result
        )
        context['task_instance'].xcom_push.assert_any_call(
            key='artists_path',
            value=result.replace('tracks_', 'artists_')
        ) 