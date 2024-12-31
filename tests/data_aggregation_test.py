import json
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from utils.data_aggregation_utils import (
    aggregate_event_data,
    aggregate_trends_data,
    generate_event_insights,
    generate_trends_insights,
    save_insights_to_bigquery
)

@pytest.fixture
def mock_storage_client():
    with patch('google.cloud.storage.Client') as mock_client:
        yield mock_client

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client

@pytest.fixture
def sample_event_data():
    events = [
        {
            'event_id': '1',
            'title': 'Test Event 1',
            'description': 'Test Description 1',
            'start_datetime': '2024-03-01T19:00:00Z',
            'location': {
                'venue_name': 'Test Venue 1'
            },
            'categories': ['Music', 'Live'],
            'price_info': {
                'min_price': 0,
                'max_price': 0
            }
        },
        {
            'event_id': '2',
            'title': 'Test Event 2',
            'description': 'Test Description 2',
            'start_datetime': '2024-03-02T20:00:00Z',
            'location': {
                'venue_name': 'Test Venue 2'
            },
            'categories': ['Music', 'Festival'],
            'price_info': {
                'min_price': 50,
                'max_price': 100
            }
        }
    ]
    return {'events': events}

@pytest.fixture
def sample_trends_data():
    topics = [
        {
            'topic': 'Test Topic 1',
            'interest_over_time': {'2024-03-01': 100}
        },
        {
            'topic': 'Test Topic 2',
            'interest_over_time': {'2024-03-01': 75}
        }
    ]
    
    interest_data = {
        'data': {
            'Test Topic 1': {'2024-03-01': 100},
            'Test Topic 2': {'2024-03-01': 75}
        }
    }
    
    return {
        'topics': topics,
        'interest': interest_data
    }

def test_aggregate_event_data(mock_storage_client, sample_event_data):
    # Mock GCS blob
    mock_blob = Mock()
    mock_blob.download_as_string.return_value = json.dumps(sample_event_data).encode()
    mock_storage_client.return_value.bucket.return_value.list_blobs.return_value = [mock_blob]
    
    # Test aggregation
    result = aggregate_event_data('test-bucket')
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'day_of_week' in result.columns
    assert 'hour_of_day' in result.columns
    assert 'is_weekend' in result.columns

def test_aggregate_trends_data(mock_storage_client, sample_trends_data):
    # Mock GCS blobs
    mock_topics_blob = Mock()
    mock_topics_blob.name = 'trends/processed/topics.json'
    mock_topics_blob.download_as_string.return_value = json.dumps(sample_trends_data['topics']).encode()
    
    mock_interest_blob = Mock()
    mock_interest_blob.name = 'trends/processed/interest.json'
    mock_interest_blob.download_as_string.return_value = json.dumps(sample_trends_data['interest']).encode()
    
    mock_storage_client.return_value.bucket.return_value.list_blobs.return_value = [
        mock_topics_blob,
        mock_interest_blob
    ]
    
    # Test aggregation
    result = aggregate_trends_data('test-bucket')
    
    assert isinstance(result, dict)
    assert 'topics' in result
    assert 'interest' in result
    assert isinstance(result['topics'], pd.DataFrame)
    assert isinstance(result['interest'], pd.DataFrame)

def test_generate_event_insights():
    # Create sample DataFrame
    data = {
        'event_id': ['1', '2'],
        'title': ['Test Event 1', 'Test Event 2'],
        'start_datetime': ['2024-03-01T19:00:00Z', '2024-03-02T20:00:00Z'],
        'day_of_week': ['Friday', 'Saturday'],
        'hour_of_day': [19, 20],
        'is_weekend': [False, True],
        'price_info.min_price': [0, 50],
        'categories': [['Music', 'Live'], ['Music', 'Festival']],
        'location.venue_name': ['Test Venue 1', 'Test Venue 2']
    }
    events_df = pd.DataFrame(data)
    
    # Test insights generation
    insights = generate_event_insights(events_df)
    
    assert isinstance(insights, dict)
    assert insights['total_events'] == 2
    assert 'events_by_day' in insights
    assert 'events_by_hour' in insights
    assert 'weekend_vs_weekday' in insights
    assert 'price_distribution' in insights
    assert 'popular_categories' in insights
    assert 'popular_venues' in insights

def test_generate_trends_insights():
    # Create sample DataFrames
    topics_data = {
        'topic': ['Test Topic 1', 'Test Topic 2'],
        'interest_over_time': [{'2024-03-01': 100}, {'2024-03-01': 75}]
    }
    topics_df = pd.DataFrame(topics_data)
    
    interest_data = {
        'topic': ['Test Topic 1', 'Test Topic 1', 'Test Topic 2', 'Test Topic 2'],
        'date': ['2024-03-01', '2024-03-02', '2024-03-01', '2024-03-02'],
        'interest_value': [100, 90, 75, 80]
    }
    interest_df = pd.DataFrame(interest_data)
    
    trends_data = {
        'topics': topics_df,
        'interest': interest_df
    }
    
    # Test insights generation
    insights = generate_trends_insights(trends_data)
    
    assert isinstance(insights, dict)
    assert insights['total_topics'] == 2
    assert 'top_topics_by_interest' in insights
    assert 'interest_distribution' in insights

def test_save_insights_to_bigquery(mock_bigquery_client):
    # Sample insights
    insights = {
        'total_events': 2,
        'events_by_day': {'Friday': 1, 'Saturday': 1},
        'price_distribution': {'free': 1, 'paid': 1}
    }
    
    # Test saving to BigQuery
    save_insights_to_bigquery(
        insights,
        'test-project',
        'test-dataset',
        'test-table'
    )
    
    # Verify BigQuery client was called correctly
    assert mock_bigquery_client.called
    mock_job = mock_bigquery_client.return_value.load_table_from_json
    assert mock_job.called
    
    # Verify job configuration
    job_config = mock_job.call_args[1]['job_config']
    assert job_config.source_format == 'NEWLINE_DELIMITED_JSON'
    assert job_config.write_disposition == 'WRITE_APPEND' 