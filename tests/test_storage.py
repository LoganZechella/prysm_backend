import pytest
from datetime import datetime, timedelta
import json
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
from decimal import Decimal

from app.storage.s3 import S3Storage
from app.storage.dynamodb import DynamoDBStorage

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
def s3_storage(aws_credentials):
    """Create S3 storage instance with mocked S3"""
    with mock_aws():
        storage = S3Storage('test-bucket')
        yield storage

@pytest.fixture
def dynamodb_storage(aws_credentials):
    """Create DynamoDB storage instance with mocked DynamoDB"""
    with mock_aws():
        storage = DynamoDBStorage('test-events')
        yield storage

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
async def test_s3_store_raw_event(s3_storage, sample_event):
    """Test storing raw event in S3"""
    event_id = await s3_storage.store_raw_event(sample_event, "test")
    assert event_id == sample_event["event_id"]
    
    # Verify event was stored
    stored_event = await s3_storage.get_raw_event(event_id, "test")
    assert stored_event["event_id"] == event_id
    assert stored_event["title"] == sample_event["title"]

@pytest.mark.asyncio
async def test_s3_store_image(s3_storage, sample_image):
    """Test storing image in S3"""
    image_url = "https://test.com/image.jpg"
    stored_url = await s3_storage.store_image(image_url, sample_image, "test123")
    assert stored_url.startswith("s3://")
    
    # Verify image was stored
    stored_image = await s3_storage.get_image(stored_url)
    assert stored_image == sample_image

@pytest.mark.asyncio
async def test_s3_list_events(s3_storage, sample_event):
    """Test listing events from S3"""
    # Store multiple events
    events = []
    for i in range(3):
        event = sample_event.copy()
        event["event_id"] = f"test{i}"
        event["start_datetime"] = (datetime.utcnow() + timedelta(days=i)).isoformat()
        await s3_storage.store_processed_event(event)
        events.append(event)
    
    # Test listing with date filter
    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=2)
    filtered_events = await s3_storage.list_events(
        start_date=start_date,
        end_date=end_date
    )
    assert len(filtered_events) == 2

@pytest.mark.asyncio
async def test_dynamodb_store_processed_event(dynamodb_storage, sample_event):
    """Test storing processed event in DynamoDB"""
    event_id = await dynamodb_storage.store_processed_event(sample_event)
    assert event_id == sample_event["event_id"]
    
    # Verify event was stored
    stored_event = await dynamodb_storage.get_processed_event(event_id)
    assert stored_event["event_id"] == event_id
    assert stored_event["title"] == sample_event["title"]

@pytest.mark.asyncio
async def test_dynamodb_list_events(dynamodb_storage, sample_event):
    """Test listing events from DynamoDB"""
    # Store multiple events
    events = []
    for i in range(3):
        event = sample_event.copy()
        event["event_id"] = f"test{i}"
        event["start_datetime"] = (datetime.utcnow() + timedelta(days=i)).isoformat()
        await dynamodb_storage.store_processed_event(event)
        events.append(event)
    
    # Test listing with category filter
    filtered_events = await dynamodb_storage.list_events(
        categories=["test"]
    )
    assert len(filtered_events) == 3
    
    # Test listing with price filter
    filtered_events = await dynamodb_storage.list_events(
        price_range={"min": 5.0, "max": 15.0}
    )
    assert len(filtered_events) == 3

@pytest.mark.asyncio
async def test_dynamodb_update_event(dynamodb_storage, sample_event):
    """Test updating event in DynamoDB"""
    # Store initial event
    event_id = await dynamodb_storage.store_processed_event(sample_event)
    
    # Update event
    update_data = {
        "title": "Updated Event",
        "price_info": {
            "min_price": 15.0,
            "max_price": 25.0,
            "price_tier": "medium"
        }
    }
    success = await dynamodb_storage.update_event(event_id, update_data)
    assert success
    
    # Verify update
    updated_event = await dynamodb_storage.get_processed_event(event_id)
    assert updated_event["title"] == "Updated Event"
    assert updated_event["price_info"]["min_price"] == 15.0

@pytest.mark.asyncio
async def test_dynamodb_delete_event(dynamodb_storage, sample_event):
    """Test deleting event from DynamoDB"""
    # Store event
    event_id = await dynamodb_storage.store_processed_event(sample_event)
    
    # Delete event
    success = await dynamodb_storage.delete_event(event_id)
    assert success
    
    # Verify deletion
    deleted_event = await dynamodb_storage.get_processed_event(event_id)
    assert deleted_event is None

@pytest.mark.asyncio
async def test_dynamodb_batch_operations(dynamodb_storage, sample_event):
    """Test batch operations in DynamoDB"""
    # Create multiple events
    events = []
    for i in range(5):
        event = sample_event.copy()
        event["event_id"] = f"test{i}"
        events.append(event)
    
    # Store batch
    event_ids = await dynamodb_storage.store_batch_events(events)
    assert len(event_ids) == 5
    
    # Verify all events were stored
    for event_id in event_ids:
        stored_event = await dynamodb_storage.get_processed_event(event_id)
        assert stored_event is not None 