import pytest
from datetime import datetime, timedelta
import json
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
import yaml

from app.storage.manager import StorageManager

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
    
    config_path = tmp_path / "test_storage.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(config, f)
    
    return str(config_path)

@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for moto"""
    import os
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

@pytest.fixture
def storage_manager(aws_credentials, test_config_path):
    """Create storage manager instance with mocked AWS services"""
    with mock_aws():
        manager = StorageManager(test_config_path)
        yield manager

@pytest.fixture
def sample_event():
    """Create a sample event for testing"""
    return {
        "event_id": "test123",
        "title": "Test Event",
        "description": "A test event",
        "start_datetime": datetime.utcnow().isoformat(),
        "location": {
            "venue_name": "Test Venue",
            "address": "123 Test St",
            "city": "Test City",
            "state": "TC",
            "country": "Test Country",
            "coordinates": {
                "lat": 37.7749,
                "lng": -122.4194
            }
        },
        "categories": ["test", "event"],
        "price_info": {
            "currency": "USD",
            "min_price": 10.0,
            "max_price": 20.0,
            "price_tier": "budget"
        },
        "source": {
            "platform": "test",
            "url": "https://test.com/event",
            "last_updated": datetime.utcnow().isoformat()
        }
    }

@pytest.fixture
def sample_image():
    """Create sample image data for testing"""
    return b"test image data"

@pytest.mark.asyncio
async def test_store_raw_event(storage_manager, sample_event):
    """Test storing raw event"""
    event_id = await storage_manager.store_raw_event(sample_event, "test")
    assert event_id == sample_event["event_id"]
    
    # Verify event was stored
    stored_event = await storage_manager.get_raw_event(event_id, "test")
    assert stored_event["event_id"] == event_id
    assert stored_event["title"] == sample_event["title"]

@pytest.mark.asyncio
async def test_store_processed_event(storage_manager, sample_event):
    """Test storing processed event"""
    event_id = await storage_manager.store_processed_event(sample_event)
    assert event_id == sample_event["event_id"]
    
    # Verify event was stored in both S3 and DynamoDB
    stored_event = await storage_manager.get_processed_event(event_id)
    assert stored_event["event_id"] == event_id
    assert stored_event["title"] == sample_event["title"]

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
    """Test listing events"""
    # Store multiple events
    events = []
    for i in range(3):
        event = sample_event.copy()
        event["event_id"] = f"test{i}"
        event["start_datetime"] = (datetime.utcnow() + timedelta(days=i)).isoformat()
        await storage_manager.store_processed_event(event)
        events.append(event)
    
    # Test listing with date filter
    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=2)
    filtered_events = await storage_manager.list_events(
        start_date=start_date,
        end_date=end_date
    )
    assert len(filtered_events) == 2
    
    # Test listing with category filter
    filtered_events = await storage_manager.list_events(
        categories=["test"]
    )
    assert len(filtered_events) == 3

@pytest.mark.asyncio
async def test_update_event(storage_manager, sample_event):
    """Test updating event"""
    # Store initial event
    event_id = await storage_manager.store_processed_event(sample_event)
    
    # Update event
    update_data = {
        "title": "Updated Event",
        "price_info": {
            "min_price": 15.0,
            "max_price": 25.0,
            "price_tier": "medium"
        }
    }
    success = await storage_manager.update_event(event_id, update_data)
    assert success
    
    # Verify update in both storages
    updated_event = await storage_manager.get_processed_event(event_id)
    assert updated_event["title"] == "Updated Event"
    assert updated_event["price_info"]["min_price"] == 15.0

@pytest.mark.asyncio
async def test_delete_event(storage_manager, sample_event):
    """Test deleting event"""
    # Store event
    event_id = await storage_manager.store_processed_event(sample_event)
    
    # Delete event
    success = await storage_manager.delete_event(event_id)
    assert success
    
    # Verify deletion from both storages
    deleted_event = await storage_manager.get_processed_event(event_id)
    assert deleted_event is None

@pytest.mark.asyncio
async def test_batch_operations(storage_manager, sample_event):
    """Test batch operations"""
    # Create multiple events
    events = []
    for i in range(50):  # Test with more than one batch
        event = sample_event.copy()
        event["event_id"] = f"test{i}"
        events.append(event)
    
    # Store batch
    event_ids = await storage_manager.store_batch_events(events)
    assert len(event_ids) == 50
    
    # Verify all events were stored
    for event_id in event_ids:
        stored_event = await storage_manager.get_processed_event(event_id)
        assert stored_event is not None

@pytest.mark.asyncio
async def test_storage_fallback(storage_manager, sample_event):
    """Test storage fallback behavior"""
    # Store event only in S3
    event_id = await storage_manager.s3.store_processed_event(sample_event)
    
    # Verify fallback to S3 when not in DynamoDB
    stored_event = await storage_manager.get_processed_event(event_id)
    assert stored_event is not None
    assert stored_event["event_id"] == event_id

@pytest.mark.asyncio
async def test_error_handling(storage_manager, sample_event):
    """Test error handling in storage operations"""
    # Test DynamoDB failure
    with patch.object(storage_manager.dynamodb, 'store_processed_event', side_effect=Exception("DynamoDB error")):
        event_id = await storage_manager.store_processed_event(sample_event)
        assert event_id == sample_event["event_id"]
        
        # Verify event is still accessible through S3
        stored_event = await storage_manager.get_processed_event(event_id)
        assert stored_event is not None

@pytest.mark.asyncio
async def test_batch_size_config(storage_manager, sample_event):
    """Test batch size configuration"""
    # Create events for multiple batches
    events = []
    for i in range(60):  # More than 2 batches with default size of 25
        event = sample_event.copy()
        event["event_id"] = f"test{i}"
        events.append(event)
    
    # Store batch
    event_ids = await storage_manager.store_batch_events(events)
    assert len(event_ids) == 60
    
    # Verify all events were stored
    for event_id in event_ids:
        stored_event = await storage_manager.get_processed_event(event_id)
        assert stored_event is not None 