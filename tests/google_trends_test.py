import pytest
from unittest.mock import Mock, patch
import pandas as pd
from utils.google_trends_utils import (
    get_google_trends_client,
    fetch_trending_topics,
    fetch_topic_interest,
    fetch_related_topics,
    upload_to_gcs
)

@pytest.fixture
def mock_trends_client():
    return Mock()

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client

def test_get_google_trends_client():
    with patch('utils.google_trends_utils.TrendReq') as mock_trends:
        client = get_google_trends_client()
        assert mock_trends.called
        assert mock_trends.call_args[1]['hl'] == 'en-US'
        assert mock_trends.call_args[1]['tz'] == 360

def test_fetch_trending_topics(mock_trends_client):
    # Mock trending searches response
    mock_trends_client.trending_searches.return_value = ['topic1', 'topic2', 'topic3']
    
    # Mock interest over time response
    mock_interest_data = pd.DataFrame({
        'topic1': [10, 20, 30],
        'topic2': [15, 25, 35],
        'topic3': [5, 15, 25]
    })
    mock_trends_client.interest_over_time.return_value = mock_interest_data
    
    topics = fetch_trending_topics(mock_trends_client)
    
    assert len(topics) > 0
    assert all(isinstance(topic, dict) for topic in topics)
    assert all('topic' in topic for topic in topics)
    assert all('interest_over_time' in topic for topic in topics)
    assert all('timestamp' in topic for topic in topics)

def test_fetch_topic_interest(mock_trends_client):
    # Mock interest over time response
    mock_interest_data = pd.DataFrame({
        'topic1': [10, 20, 30],
        'topic2': [15, 25, 35]
    })
    mock_trends_client.interest_over_time.return_value = mock_interest_data
    
    topics = ['topic1', 'topic2']
    interest_data = fetch_topic_interest(mock_trends_client, topics)
    
    assert isinstance(interest_data, dict)
    assert all(topic in interest_data for topic in topics)
    assert mock_trends_client.build_payload.called
    assert mock_trends_client.interest_over_time.called

def test_fetch_related_topics(mock_trends_client):
    # Mock related topics response
    mock_related_data = {
        'topic1': {
            'rising': pd.DataFrame({
                'topic': ['related1', 'related2'],
                'value': [100, 50]
            })
        }
    }
    mock_trends_client.related_topics.return_value = mock_related_data
    
    topics = ['topic1']
    related_data = fetch_related_topics(mock_trends_client, topics)
    
    assert isinstance(related_data, dict)
    assert 'topic1' in related_data
    assert isinstance(related_data['topic1'], list)
    assert mock_trends_client.build_payload.called
    assert mock_trends_client.related_topics.called

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