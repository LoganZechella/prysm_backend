import pytest
from unittest.mock import Mock, patch
from utils.linkedin_utils import (
    get_linkedin_client,
    fetch_profile_info,
    fetch_connections,
    fetch_recent_activity,
    upload_to_gcs
)

@pytest.fixture
def mock_linkedin_client():
    return {
        'access_token': 'test_token',
        'headers': {
            'Authorization': 'Bearer test_token',
            'Content-Type': 'application/json',
            'X-Restli-Protocol-Version': '2.0.0'
        }
    }

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client

def test_get_linkedin_client():
    with patch('os.getenv', return_value='test_token'):
        client = get_linkedin_client()
        assert isinstance(client, dict)
        assert 'access_token' in client
        assert 'headers' in client
        assert client['headers']['Authorization'] == 'Bearer test_token'

def test_fetch_profile_info(mock_linkedin_client):
    mock_response = {
        'id': '12345',
        'firstName': 'Test',
        'lastName': 'User'
    }
    
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status = Mock()
        
        profile = fetch_profile_info(mock_linkedin_client)
        
        assert profile == mock_response
        mock_get.assert_called_once_with(
            'https://api.linkedin.com/v2/me',
            headers=mock_linkedin_client['headers']
        )

def test_fetch_connections(mock_linkedin_client):
    mock_response = {
        'elements': [
            {'id': '1', 'firstName': 'Connection1'},
            {'id': '2', 'firstName': 'Connection2'}
        ]
    }
    
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status = Mock()
        
        connections = fetch_connections(mock_linkedin_client)
        
        assert connections == mock_response
        mock_get.assert_called_once_with(
            'https://api.linkedin.com/v2/connections',
            headers=mock_linkedin_client['headers']
        )

def test_fetch_recent_activity(mock_linkedin_client):
    mock_response = {
        'elements': [
            {'id': '1', 'type': 'POST'},
            {'id': '2', 'type': 'SHARE'}
        ]
    }
    
    with patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status = Mock()
        
        activity = fetch_recent_activity(mock_linkedin_client)
        
        assert activity == mock_response
        assert mock_get.called

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