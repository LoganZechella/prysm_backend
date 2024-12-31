import os
import pytest
from unittest.mock import patch, MagicMock
from utils.linkedin_utils import (
    get_linkedin_client,
    fetch_company_info,
    fetch_company_updates,
    fetch_job_postings,
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
def test_fetch_company_info(mock_get):
    """Test fetching company information"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'id': '12345',
        'name': 'Test Company',
        'description': 'A test company',
        'industries': ['Technology', 'Software'],
        'specialties': ['AI', 'Machine Learning'],
        'locations': [{'city': 'San Francisco', 'country': 'US'}],
        'staffCount': 500,
        'foundedOn': '2020-01-01',
        'website': 'https://testcompany.com',
        'status': 'ACTIVE',
        'vanityName': 'testcompany'
    }
    mock_get.return_value = mock_response
    
    auth = get_linkedin_client()
    company = fetch_company_info(auth, '12345')
    
    assert company['company_id'] == '12345'
    assert company['name'] == 'Test Company'
    assert company['description'] == 'A test company'
    assert 'Technology' in company['industries']
    assert 'AI' in company['specialties']
    assert company['staff_count'] == 500
    assert company['website'] == 'https://testcompany.com'
    assert 'timestamp' in company

@patch('requests.get')
def test_fetch_company_updates(mock_get):
    """Test fetching company updates"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'elements': [{
            'id': 'update123',
            'specificContent': {
                'com.linkedin.ugc.ShareContent': {
                    'shareCommentary': {
                        'text': 'Test update content'
                    }
                }
            },
            'social': {
                'totalLikes': 100,
                'totalComments': 50,
                'totalShares': 25
            },
            'created': {'time': '2024-01-01T12:00:00Z'}
        }]
    }
    mock_get.return_value = mock_response
    
    auth = get_linkedin_client()
    updates = fetch_company_updates(auth, '12345')
    
    assert len(updates) == 1
    assert updates[0]['update_id'] == 'update123'
    assert updates[0]['company_id'] == '12345'
    assert updates[0]['content'] == 'Test update content'
    assert updates[0]['engagement']['likes'] == 100
    assert updates[0]['engagement']['comments'] == 50
    assert updates[0]['engagement']['shares'] == 25
    assert updates[0]['posted_at'] == '2024-01-01T12:00:00Z'
    assert 'timestamp' in updates[0]

@patch('requests.get')
def test_fetch_job_postings(mock_get):
    """Test fetching job postings"""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'elements': [{
            'id': 'job123',
            'title': 'Software Engineer',
            'description': 'Test job description',
            'locationDescription': 'San Francisco, CA',
            'workRemoteAllowed': True,
            'experienceLevel': 'Senior',
            'employmentType': 'Full-time',
            'industries': ['Software'],
            'applies': 50,
            'views': 1000,
            'listedAt': '2024-01-01T12:00:00Z'
        }]
    }
    mock_get.return_value = mock_response
    
    auth = get_linkedin_client()
    jobs = fetch_job_postings(auth, '12345')
    
    assert len(jobs) == 1
    assert jobs[0]['job_id'] == 'job123'
    assert jobs[0]['company_id'] == '12345'
    assert jobs[0]['title'] == 'Software Engineer'
    assert jobs[0]['description'] == 'Test job description'
    assert jobs[0]['location'] == 'San Francisco, CA'
    assert jobs[0]['remote_allowed'] is True
    assert jobs[0]['experience_level'] == 'Senior'
    assert jobs[0]['employment_type'] == 'Full-time'
    assert 'Software' in jobs[0]['industries']
    assert jobs[0]['applies'] == 50
    assert jobs[0]['views'] == 1000
    assert jobs[0]['listed_at'] == '2024-01-01T12:00:00Z'
    assert 'timestamp' in jobs[0]

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