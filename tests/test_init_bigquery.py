import pytest
from unittest.mock import Mock, patch
from google.cloud import bigquery
from scripts.init_bigquery import create_dataset_if_not_exists

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client

def test_create_dataset_if_not_exists_already_exists(mock_bigquery_client):
    """Test handling of existing dataset."""
    # Mock dataset exists
    mock_bigquery_client.return_value.get_dataset.return_value = Mock()
    
    # Test dataset creation
    create_dataset_if_not_exists('test-project', 'test-dataset')
    
    # Verify client was called correctly
    assert mock_bigquery_client.called
    mock_bigquery_client.return_value.get_dataset.assert_called_once()
    mock_bigquery_client.return_value.create_dataset.assert_not_called()

def test_create_dataset_if_not_exists_not_found(mock_bigquery_client):
    """Test creating a new dataset."""
    # Mock dataset doesn't exist
    mock_bigquery_client.return_value.get_dataset.side_effect = Exception("Not found")
    mock_bigquery_client.return_value.create_dataset.return_value = Mock()
    
    # Test dataset creation
    create_dataset_if_not_exists('test-project', 'test-dataset')
    
    # Verify client was called correctly
    assert mock_bigquery_client.called
    mock_bigquery_client.return_value.get_dataset.assert_called_once()
    mock_bigquery_client.return_value.create_dataset.assert_called_once()

def test_create_dataset_if_not_exists_other_error(mock_bigquery_client):
    """Test handling of other errors."""
    # Mock other error
    mock_bigquery_client.return_value.get_dataset.side_effect = Exception("Other error")
    
    # Test should raise the error
    with pytest.raises(Exception) as exc_info:
        create_dataset_if_not_exists('test-project', 'test-dataset')
    assert "Other error" in str(exc_info.value)

def test_create_dataset_if_not_exists_with_location(mock_bigquery_client):
    """Test creating a dataset with custom location."""
    # Mock dataset doesn't exist
    mock_bigquery_client.return_value.get_dataset.side_effect = Exception("Not found")
    mock_bigquery_client.return_value.create_dataset.return_value = Mock()
    
    # Test dataset creation with custom location
    create_dataset_if_not_exists('test-project', 'test-dataset', 'EU')
    
    # Verify client was called correctly
    assert mock_bigquery_client.called
    mock_bigquery_client.return_value.get_dataset.assert_called_once()
    
    # Verify dataset was created with correct location
    create_dataset_call = mock_bigquery_client.return_value.create_dataset.call_args
    dataset = create_dataset_call[0][0]
    assert isinstance(dataset, bigquery.Dataset)
    assert dataset.location == 'EU' 