import pytest
from unittest.mock import Mock, patch
from google.cloud import bigquery
from config.bigquery_schemas import (
    EVENT_INSIGHTS_SCHEMA,
    TRENDS_INSIGHTS_SCHEMA,
    create_insights_tables
)

def test_event_insights_schema():
    """Test event insights schema structure."""
    # Verify required fields
    required_fields = {
        'total_events',
        'events_by_day',
        'events_by_hour',
        'weekend_vs_weekday',
        'price_distribution',
        'popular_categories',
        'popular_venues',
        'timestamp'
    }
    
    schema_fields = {field.name for field in EVENT_INSIGHTS_SCHEMA}
    assert required_fields.issubset(schema_fields)
    
    # Verify field types
    field_types = {field.name: field.field_type for field in EVENT_INSIGHTS_SCHEMA}
    assert field_types['total_events'] == 'INTEGER'
    assert field_types['events_by_day'] == 'RECORD'
    assert field_types['events_by_hour'] == 'RECORD'
    assert field_types['weekend_vs_weekday'] == 'RECORD'
    assert field_types['price_distribution'] == 'RECORD'
    assert field_types['popular_categories'] == 'RECORD'
    assert field_types['popular_venues'] == 'RECORD'
    assert field_types['timestamp'] == 'TIMESTAMP'

def test_trends_insights_schema():
    """Test trends insights schema structure."""
    # Verify required fields
    required_fields = {
        'total_topics',
        'top_topics_by_interest',
        'interest_distribution',
        'timestamp'
    }
    
    schema_fields = {field.name for field in TRENDS_INSIGHTS_SCHEMA}
    assert required_fields.issubset(schema_fields)
    
    # Verify field types
    field_types = {field.name: field.field_type for field in TRENDS_INSIGHTS_SCHEMA}
    assert field_types['total_topics'] == 'INTEGER'
    assert field_types['top_topics_by_interest'] == 'RECORD'
    assert field_types['interest_distribution'] == 'RECORD'
    assert field_types['timestamp'] == 'TIMESTAMP'

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client

def test_create_insights_tables(mock_bigquery_client):
    """Test creating BigQuery tables."""
    # Mock successful table creation
    mock_bigquery_client.return_value.create_table.return_value = None
    
    # Test table creation
    create_insights_tables('test-project', 'test-dataset')
    
    # Verify client was called correctly
    assert mock_bigquery_client.called
    create_table_calls = mock_bigquery_client.return_value.create_table.call_args_list
    assert len(create_table_calls) == 2  # Two tables should be created
    
    # Verify table configurations
    for call in create_table_calls:
        table = call[0][0]
        assert isinstance(table, bigquery.Table)
        assert table.project == 'test-project'
        assert table.dataset_id == 'test-dataset'
        assert table.table_id in ['event_insights', 'trends_insights']
        
        # Verify time partitioning is set
        assert table.time_partitioning is not None
        assert isinstance(table.time_partitioning, bigquery.TimePartitioning)

def test_create_insights_tables_already_exists(mock_bigquery_client):
    """Test handling of already existing tables."""
    # Mock table already exists error
    mock_bigquery_client.return_value.create_table.side_effect = Exception("Already Exists")
    
    # Test should not raise an error
    try:
        create_insights_tables('test-project', 'test-dataset')
    except Exception as e:
        pytest.fail(f"create_insights_tables raised {e} unexpectedly!")

def test_create_insights_tables_other_error(mock_bigquery_client):
    """Test handling of other errors."""
    # Mock other error
    mock_bigquery_client.return_value.create_table.side_effect = Exception("Other error")
    
    # Test should raise the error
    with pytest.raises(Exception) as exc_info:
        create_insights_tables('test-project', 'test-dataset')
    assert "Other error" in str(exc_info.value) 