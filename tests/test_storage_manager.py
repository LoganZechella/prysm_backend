import pytest
from datetime import datetime, timedelta
import json
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_s3
import yaml

from app.utils.storage import StorageManager
from app.models.event import EventModel as Event

@pytest.fixture
def test_config_path(tmp_path):
    """Create a temporary config file"""
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'region': 'us-west-2',
            'lifecycle_rules': {
                'raw_data_retention_days': 90,
                'processed_data_retention_days': 30
            },
            'versioning': 'enabled'
        },
        'dynamodb': {
            'table_name': 'test-events',
            'region': 'us-west-2',
            'capacity': {
                'read_units': 5,
                'write_units': 5
            },
            'indexes': [
                {
                    'name': 'start_datetime-index',
                    'key': 'start_datetime',
                    'type': 'hash'
                },
                {
                    'name': 'location-index',
                    'key': 'location_hash',
                    'type': 'hash'
                }
            ],
            'stream': {
                'enabled': True,
                'view_type': 'NEW_AND_OLD_IMAGES'
            }
        },
        'manager': {
            'default_region': 'us-west-2',
            'fallback_strategy': 's3',
            'batch_size': 25
        }
    }
    
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f)
    
    return str(config_file)

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    with patch.dict('os.environ', {
        'AWS_ACCESS_KEY_ID': 'testing',
        'AWS_SECRET_ACCESS_KEY': 'testing',
        'AWS_SECURITY_TOKEN': 'testing',
        'AWS_SESSION_TOKEN': 'testing',
        'AWS_DEFAULT_REGION': 'us-west-2'
    }):
        yield

@pytest.fixture
def storage_manager(aws_credentials, test_config_path):
    """Create a storage manager with mocked AWS credentials."""
    with mock_s3():
        s3 = boto3.client('s3', region_name='us-west-2')
        s3.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
        )
        manager = StorageManager()
        yield manager

@pytest.fixture
def sample_event():
    """Create a sample event for testing."""
    return {
        'event_id': 'test-123',
        'title': 'Test Event',
        'description': 'A test event',
        'start_datetime': datetime.now().isoformat(),
        'end_datetime': (datetime.now() + timedelta(hours=2)).isoformat(),
        'location': {
            'venue_name': 'Test Venue',
            'address': '123 Test St',
            'city': 'Test City',
            'state': 'TS',
            'country': 'Test Country',
            'postal_code': '12345',
            'lat': 37.7749,
            'lon': -122.4194
        },
        'categories': ['test', 'event'],
        'price_info': {
            'type': 'fixed',
            'amount': 10.0,
            'currency': 'USD'
        },
        'source': {
            'platform': 'test',
            'url': 'http://test.com/event/123'
        }
    }

@pytest.fixture
def sample_image():
    """Create sample image data for testing"""
    return b"test image data"

@pytest.mark.asyncio
async def test_store_raw_event(storage_manager, sample_event):
    """Test storing a raw event."""
    key = await storage_manager.store_raw_event(sample_event, 'test')
    assert key.startswith('test/raw/')
    assert key.endswith('.json')
    
    stored_event = await storage_manager.get_raw_event(key)
    assert stored_event['event_id'] == sample_event['event_id']
    assert stored_event['source'] == 'test'
    assert 'ingestion_timestamp' in stored_event

@pytest.mark.asyncio
async def test_store_processed_event(storage_manager, sample_event):
    """Test storing a processed event."""
    key = await storage_manager.store_processed_event(sample_event)
    assert key.startswith('processed/')
    assert key.endswith('.json')
    
    stored_event = await storage_manager.get_processed_event(key)
    assert stored_event['event_id'] == sample_event['event_id']
    assert 'processing_timestamp' in stored_event

@pytest.mark.asyncio
async def test_store_image(storage_manager, sample_image):
    """Test storing image"""
    image_url = "https://test.com/image.jpg"
    stored_url = await storage_manager.store_image(image_url, sample_image, "test123")
    assert stored_url.startswith("s3://")
    
    # Verify image was stored
    stored_image = await storage_manager.get_image(stored_url)
    assert stored_image == sample_image

@pytest.mark.asyncio
async def test_list_events(storage_manager, sample_event):
    """Test listing events."""
    # Store some test events
    await storage_manager.store_raw_event(sample_event, 'test')
    await storage_manager.store_raw_event(sample_event, 'test')
    
    # List all events
    raw_keys = await storage_manager.list_raw_events()
    assert len(raw_keys) == 2
    assert all(key.startswith('test/raw/') for key in raw_keys)
    
    # List with source filter
    filtered_keys = await storage_manager.list_raw_events(source='test')
    assert len(filtered_keys) == 2
    assert all(key.startswith('test/raw/') for key in filtered_keys)
    
    # List with date filter
    future_keys = await storage_manager.list_raw_events(
        start_date=datetime.now() + timedelta(days=1)
    )
    assert len(future_keys) == 0

@pytest.mark.asyncio
async def test_update_event(storage_manager, sample_event):
    """Test updating an event."""
    # First store the event
    key = await storage_manager.store_processed_event(sample_event)
    
    # Update some fields
    updated_event = sample_event.copy()
    updated_event['title'] = 'Updated Test Event'
    updated_event['description'] = 'Updated description'
    
    # Store the update
    updated_key = await storage_manager.store_processed_event(updated_event)
    
    # Verify update
    stored_event = await storage_manager.get_processed_event(updated_key)
    assert stored_event['title'] == 'Updated Test Event'
    assert stored_event['description'] == 'Updated description'
    assert stored_event['event_id'] == sample_event['event_id']

@pytest.mark.asyncio
async def test_delete_event(storage_manager, sample_event):
    """Test deleting an event."""
    # First store the event
    key = await storage_manager.store_processed_event(sample_event)
    
    # Verify it exists
    stored_event = await storage_manager.get_processed_event(key)
    assert stored_event is not None
    
    # Delete it
    await storage_manager.delete_processed_event(key)
    
    # Verify it's gone
    deleted_event = await storage_manager.get_processed_event(key)
    assert deleted_event is None

@pytest.mark.asyncio
async def test_batch_operations(storage_manager, sample_event):
    """Test batch operations."""
    # Create multiple events
    events = []
    for i in range(3):
        event = sample_event.copy()
        event['event_id'] = f'test-{i}'
        event['title'] = f'Test Event {i}'
        events.append(event)
    
    # Store batch
    keys = await storage_manager.store_processed_events_batch(events)
    assert len(keys) == 3
    
    # Retrieve batch
    stored_events = await storage_manager.get_processed_events_batch(keys)
    assert len(stored_events) == 3
    assert all(e['title'].startswith('Test Event') for e in stored_events)
    
    # Delete batch
    await storage_manager.delete_processed_events_batch(keys)
    
    # Verify deletion
    remaining = await storage_manager.get_processed_events_batch(keys)
    assert all(e is None for e in remaining)

@pytest.mark.asyncio
async def test_storage_fallback(storage_manager, sample_event):
    """Test storage fallback behavior."""
    # Simulate S3 failure
    with patch.object(storage_manager.s3_client, 'put_object', side_effect=Exception('S3 error')):
        # Should use fallback storage
        key = await storage_manager.store_processed_event(sample_event)
        assert key is not None
        
        # Should still be able to retrieve
        stored_event = await storage_manager.get_processed_event(key)
        assert stored_event is not None
        assert stored_event['event_id'] == sample_event['event_id']

@pytest.mark.asyncio
async def test_error_handling(storage_manager, sample_event):
    """Test error handling."""
    # Test invalid event
    invalid_event = {'invalid': 'data'}
    with pytest.raises(ValueError):
        await storage_manager.store_processed_event(invalid_event)
    
    # Test invalid key
    with pytest.raises(ValueError):
        await storage_manager.get_processed_event('')
    
    # Test non-existent key
    non_existent = await storage_manager.get_processed_event('non-existent-key')
    assert non_existent is None

@pytest.mark.asyncio
async def test_batch_size_config(storage_manager, sample_event):
    """Test batch size configuration."""
    # Create many events
    events = []
    for i in range(25):  # More than default batch size
        event = sample_event.copy()
        event['event_id'] = f'test-{i}'
        events.append(event)
    
    # Store batch (should automatically split into multiple batches)
    keys = await storage_manager.store_processed_events_batch(events)
    assert len(keys) == 25
    
    # Retrieve all events
    stored_events = await storage_manager.get_processed_events_batch(keys)
    assert len(stored_events) == 25
    assert all(e is not None for e in stored_events)

@mock_s3
def test_storage_manager():
    """Test basic storage manager functionality."""
    storage = StorageManager()
    assert storage is not None

    # ... existing code ... 