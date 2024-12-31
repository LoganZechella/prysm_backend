import os
import pytest
from unittest.mock import patch, MagicMock
from utils.linkedin_utils import (
    get_linkedin_client,
    fetch_profile_info,
    fetch_connections,
    fetch_recent_activity,
    upload_to_gcs
)

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Set up mock environment variables for testing"""
    monkeypatch.setenv('LINKEDIN_CLIENT_ID', 'test_client_id')
    monkeypatch.setenv('LINKEDIN_CLIENT_SECRET', 'test_client_secret')
    monkeypatch.setenv('LINKEDIN_ACCESS_TOKEN', 'test_access_token')

def test_get_linkedin_client():
    """Test LinkedIn client creation with mock credentials"""
    client = get_linkedin_client()
    assert client['access_token'] == 'test_access_token'
    assert client['headers']['Authorization'] == 'Bearer test_access_token'
    assert client['headers']['X-Restli-Protocol-Version'] == '2.0.0'
    assert client['headers']['Content-Type'] == 'application/json'

@patch('requests.get')
def test_fetch_profile_info(mock_get):
    """Test fetching profile information"""
    # Mock response for profile info
    mock_profile_response = MagicMock()
    mock_profile_response.json.return_value = {
        'id': '123456',
        'localizedFirstName': 'John',
        'localizedLastName': 'Doe',
        'profilePicture': {
            'displayImage~': {
                'elements': [{
                    'identifiers': [{
                        'identifier': 'http://example.com/profile.jpg'
                    }]
                }]
            }
        }
    }
    
    # Mock response for email
    mock_email_response = MagicMock()
    mock_email_response.json.return_value = {
        'elements': [{
            'handle~': {
                'emailAddress': 'john.doe@example.com'
            }
        }]
    }
    
    mock_get.side_effect = [mock_profile_response, mock_email_response]
    
    auth = get_linkedin_client()
    profile = fetch_profile_info(auth)
    
    assert profile['profile_id'] == '123456'
    assert profile['first_name'] == 'John'
    assert profile['last_name'] == 'Doe'
    assert profile['email'] == 'john.doe@example.com'
    assert profile['profile_picture_url'] == 'http://example.com/profile.jpg'
    assert 'timestamp' in profile

@patch('requests.get')
def test_fetch_connections(mock_get):
    """Test fetching connections"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'elements': [{
            'miniProfile': {
                'id': '789012',
                'firstName': {'localized': {'en_US': 'Jane'}},
                'lastName': {'localized': {'en_US': 'Smith'}},
                'occupation': {'localized': {'en_US': 'Software Engineer'}},
                'publicIdentifier': 'jane-smith'
            }
        }]
    }
    mock_get.return_value = mock_response
    
    auth = get_linkedin_client()
    connections = fetch_connections(auth)
    
    assert len(connections) == 1
    assert connections[0]['profile_id'] == '789012'
    assert connections[0]['first_name'] == 'Jane'
    assert connections[0]['last_name'] == 'Smith'
    assert connections[0]['occupation'] == 'Software Engineer'
    assert connections[0]['public_identifier'] == 'jane-smith'
    assert 'timestamp' in connections[0]

@patch('requests.get')
def test_fetch_recent_activity(mock_get):
    """Test fetching recent activity"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'elements': [{
            'id': 'activity123',
            'activity': 'SHARE',
            'specificContent': {
                'com.linkedin.ugc.ShareContent': {
                    'message': {
                        'text': 'Test post content'
                    }
                }
            },
            'created': {'time': '2024-01-01T12:00:00Z'},
            'lastModified': {'time': '2024-01-01T12:00:00Z'}
        }]
    }
    mock_get.return_value = mock_response
    
    auth = get_linkedin_client()
    activities = fetch_recent_activity(auth)
    
    assert len(activities) == 1
    assert activities[0]['activity_id'] == 'activity123'
    assert activities[0]['activity_type'] == 'SHARE'
    assert activities[0]['message'] == 'Test post content'
    assert activities[0]['created_time'] == '2024-01-01T12:00:00Z'
    assert activities[0]['last_modified_time'] == '2024-01-01T12:00:00Z'
    assert 'timestamp' in activities[0]

@patch('google.cloud.storage.Client')
def test_upload_to_gcs(mock_storage_client):
    """Test uploading data to GCS"""
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage_client.return_value.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    
    test_data = {'test': 'data'}
    bucket_name = 'test-bucket'
    blob_path = 'test/path.json'
    
    result = upload_to_gcs(test_data, bucket_name, blob_path)
    
    assert result == f'gs://{bucket_name}/{blob_path}'
    mock_blob.upload_from_string.assert_called_once() 