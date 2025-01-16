"""Tests for storage implementations."""

import pytest
from datetime import datetime, timedelta
import uuid
import os
from typing import Dict, Any
from unittest.mock import AsyncMock, patch, MagicMock
import json
from aioresponses import aioresponses

from app.storage.dynamodb import DynamoDBStorage
from app.storage.s3 import S3Storage
from app.storage.manager import StorageManager

@pytest.fixture
def event_data():
    """Sample event data for testing."""
    return {
        'event_id': str(uuid.uuid4()),
        'source': 'test',
        'title': 'Test Event',
        'description': 'Test Description',
        'start_datetime': datetime.utcnow().isoformat(),
        'end_datetime': (datetime.utcnow() + timedelta(hours=2)).isoformat(),
        'location': {'city': 'Test City', 'country': 'Test Country'},
        'price_info': {'currency': 'USD', 'min': 10, 'max': 50},
        'image_url': 'https://test.com/image.jpg',
        'url': 'https://test.com/event',
        'raw_ingestion_time': datetime.utcnow().isoformat(),
        'pipeline_version': '1.0'
    }

@pytest.fixture
def raw_event_batch(event_data):
    """Batch of raw events for testing."""
    return [
        {**event_data, 'event_id': str(uuid.uuid4())}
        for _ in range(5)
    ]

@pytest.fixture
def mock_aws():
    """Mock AWS services."""
    with patch('aioboto3.Session', autospec=True) as mock_session:
        # Mock DynamoDB
        mock_dynamodb = AsyncMock()
        mock_table = AsyncMock()
        mock_batch_writer = AsyncMock()
        mock_batch_writer.__aenter__.return_value = mock_batch_writer
        mock_batch_writer.__aexit__.return_value = None
        mock_table.batch_writer.return_value = mock_batch_writer
        mock_dynamodb.Table.return_value = mock_table
        
        # Mock S3
        mock_s3 = AsyncMock()
        mock_s3.head_bucket = AsyncMock()
        mock_s3.put_object = AsyncMock()
        mock_s3.get_object = AsyncMock()
        mock_s3.list_objects_v2 = AsyncMock()
        
        # Set up session
        mock_session_instance = mock_session.return_value
        mock_session_instance.resource.return_value.__aenter__.return_value = mock_dynamodb
        mock_session_instance.client.return_value.__aenter__.return_value = mock_s3
        
        yield mock_session_instance

@pytest.fixture
def storage_manager(event_data):
    """Create a storage manager with mocked AWS clients."""
    manager = StorageManager()
    manager.dynamodb = AsyncMock()
    manager.s3 = AsyncMock()
    
    # Mock DynamoDB methods
    manager.dynamodb.store_batch_events = AsyncMock(
        side_effect=lambda events, is_raw: [
            event.get('event_id', str(uuid.uuid4()))
            for event in events
        ]
    )
    manager.dynamodb.get_raw_event = AsyncMock(
        side_effect=lambda event_id: {**event_data, 'event_id': event_id} if event_id != 'invalid-id' else None
    )
    manager.dynamodb.list_raw_events = AsyncMock(
        return_value=[{**event_data, 'event_id': str(uuid.uuid4())} for _ in range(5)]
    )
    manager.dynamodb.store_processed_event = AsyncMock(
        side_effect=lambda event: event.get('event_id', str(uuid.uuid4()))
    )
    
    # Mock S3 methods with similar behavior
    manager.s3.store_batch_events = AsyncMock(
        side_effect=lambda events, is_raw: [
            event.get('event_id', str(uuid.uuid4()))
            for event in events
        ]
    )
    manager.s3.get_raw_event = AsyncMock(
        side_effect=lambda event_id: {**event_data, 'event_id': event_id} if event_id != 'invalid-id' else None
    )
    manager.s3.list_raw_events = AsyncMock(
        return_value=[{**event_data, 'event_id': str(uuid.uuid4())} for _ in range(5)]
    )
    manager.s3.store_processed_event = AsyncMock(
        side_effect=lambda event: event.get('event_id', str(uuid.uuid4()))
    )
    
    return manager

@pytest.mark.asyncio
async def test_store_and_get_raw_event(storage_manager, event_data, mock_aws):
    """Test storing and retrieving a raw event."""
    # Initialize storage manager
    await storage_manager.initialize()
    
    # Mock responses
    mock_aws.client.return_value.__aenter__.return_value.get_object.return_value = {
        'Body': AsyncMock(
            read=AsyncMock(return_value=json.dumps(event_data).encode())
        )
    }
    
    # Store raw event
    stored_ids = await storage_manager.store_batch_events([event_data], is_raw=True)
    assert len(stored_ids) == 1
    assert stored_ids[0] == event_data['event_id']
    
    # Retrieve raw event
    retrieved_event = await storage_manager.get_raw_event(event_data['event_id'])
    assert retrieved_event is not None
    assert retrieved_event['event_id'] == event_data['event_id']
    assert retrieved_event['title'] == event_data['title']

@pytest.mark.asyncio
async def test_store_batch_raw_events(storage_manager, raw_event_batch, mock_aws):
    """Test storing a batch of raw events."""
    await storage_manager.initialize()
    stored_ids = await storage_manager.store_batch_events(raw_event_batch, is_raw=True)
    assert len(stored_ids) == len(raw_event_batch)
    
    # Mock responses for each event
    for event in raw_event_batch:
        mock_aws.client.return_value.__aenter__.return_value.get_object.return_value = {
            'Body': AsyncMock(
                read=AsyncMock(return_value=json.dumps(event).encode())
            )
        }
        
        # Verify event was stored
        retrieved_event = await storage_manager.get_raw_event(event['event_id'])
        assert retrieved_event is not None
        assert retrieved_event['event_id'] == event['event_id']

@pytest.mark.asyncio
async def test_list_raw_events(storage_manager, raw_event_batch, mock_aws):
    """Test listing raw events with filters."""
    await storage_manager.initialize()
    
    # Store batch events
    await storage_manager.store_batch_events(raw_event_batch, is_raw=True)
    
    # Mock list response
    mock_aws.client.return_value.__aenter__.return_value.list_objects_v2.return_value = {
        'Contents': [
            {'Key': f"raw-events/test/{event['event_id']}.json"}
            for event in raw_event_batch
        ]
    }
    
    # Mock get_object responses
    mock_aws.client.return_value.__aenter__.return_value.get_object.return_value = {
        'Body': AsyncMock(
            read=AsyncMock(return_value=json.dumps(raw_event_batch[0]).encode())
        )
    }
    
    # List events without date filters
    events = await storage_manager.list_raw_events('test')
    assert len(events) >= len(raw_event_batch)
    
    # List events with date filters
    start_date = datetime.utcnow() - timedelta(hours=1)
    end_date = datetime.utcnow() + timedelta(hours=1)
    filtered_events = await storage_manager.list_raw_events(
        'test',
        start_date=start_date,
        end_date=end_date
    )
    assert len(filtered_events) >= len(raw_event_batch)

@pytest.mark.asyncio
async def test_store_processed_event(storage_manager, event_data, mock_aws):
    """Test storing a processed event."""
    await storage_manager.initialize()
    
    processed_event = {
        **event_data,
        'processed_at': datetime.utcnow().isoformat(),
        'processing_version': '1.0'
    }
    
    # Store processed event
    event_id = await storage_manager.store_processed_event(processed_event)
    assert event_id == processed_event['event_id']

@pytest.mark.asyncio
async def test_error_handling(storage_manager, mock_aws):
    """Test error handling for invalid operations."""
    await storage_manager.initialize()
    
    # Mock error responses
    mock_aws.client.return_value.__aenter__.return_value.get_object.side_effect = Exception("Not found")
    
    # Test with invalid event ID
    invalid_event = await storage_manager.get_raw_event('invalid-id')
    assert invalid_event is None
    
    # Test with invalid event data
    invalid_data = {'invalid': 'data'}
    stored_ids = await storage_manager.store_batch_events([invalid_data], is_raw=True)
    assert len(stored_ids) == 0

@pytest.mark.asyncio
async def test_dynamodb_specific(mock_aws, event_data):
    """Test DynamoDB-specific functionality."""
    dynamodb = DynamoDBStorage()
    
    # Mock the session
    mock_resource = AsyncMock()
    mock_table = AsyncMock()
    mock_batch_writer = AsyncMock()
    
    # Set up the mock chain
    dynamodb.session = AsyncMock()
    dynamodb.session.resource = AsyncMock(return_value=mock_resource)
    mock_resource.Table = AsyncMock(return_value=mock_table)
    
    # Set up batch writer mock
    mock_batch_writer.put_item = AsyncMock()
    mock_batch_writer.__aenter__ = AsyncMock(return_value=mock_batch_writer)
    mock_batch_writer.__aexit__ = AsyncMock(return_value=None)
    
    # Mock the batch_writer method to return a context manager
    mock_table.batch_writer = AsyncMock()
    mock_table.batch_writer.return_value = mock_batch_writer
    
    # Mock return values for stored IDs
    stored_ids = []  # Initialize list to store IDs
    mock_batch_writer.put_item.side_effect = lambda Item: stored_ids.append(Item['event_id'])
    
    # Test batch write with more than 25 items
    large_batch = [
        {**event_data, 'event_id': str(uuid.uuid4())}
        for _ in range(30)
    ]
    stored_result = await dynamodb.store_batch_events(large_batch, is_raw=True)
    assert len(stored_result) == len(large_batch)
    assert stored_result == stored_ids  # Verify IDs match
    
    # Verify batch writer was called correctly
    assert mock_table.batch_writer.call_count > 0
    assert mock_batch_writer.put_item.call_count == len(large_batch)

@pytest.mark.asyncio
async def test_s3_specific(mock_aws, event_data):
    """Test S3-specific functionality."""
    s3 = S3Storage()
    await s3.initialize()
    
    # Test storing with custom metadata
    event_with_metadata = {
        **event_data,
        'custom_metadata': {'test_key': 'test_value'}
    }
    stored_id = await s3.store_processed_event(event_with_metadata)
    assert stored_id == event_with_metadata['event_id'] 