"""Tests for the event pipeline service."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import json
from unittest.mock import AsyncMock

from app.services.event_pipeline import EventPipeline
from app.storage.manager import StorageManager

@pytest.fixture
def sample_event():
    """Create a sample event for testing"""
    return {
        "title": "Test Event",
        "description": "A test event description",
        "start_time": datetime.utcnow().isoformat(),
        "end_time": (datetime.utcnow().replace(hour=datetime.utcnow().hour + 2)).isoformat(),
        "location": {
            "venue_name": "Test Venue",
            "address": "123 Test St",
            "city": "Test City",
            "state": "TC",
            "latitude": 37.7749,
            "longitude": -122.4194
        },
        "price": {
            "amount": 25.0,
            "currency": "USD"
        },
        "image_url": "https://example.com/image.jpg",
        "url": "https://example.com/event",
        "event_id": "test123"
    }

@pytest.fixture
def pipeline(mock_storage):
    """Create event pipeline for testing"""
    pipeline = EventPipeline()
    pipeline.storage = mock_storage
    return pipeline

@pytest.fixture
def mock_storage():
    """Create mock storage manager"""
    storage = Mock()
    storage.store_batch_events = AsyncMock()
    storage.store_batch_events.return_value = ["test123"]
    return storage

@pytest.mark.asyncio
async def test_process_events(pipeline, sample_event):
    """Test processing a batch of events"""
    # Setup mock storage responses
    pipeline.storage.store_batch_events.return_value = ["test123"]
    pipeline.storage.store_image.return_value = "s3://bucket/images/test123.jpg"
    pipeline.storage.update_event.return_value = True
    
    # Process events
    events = [sample_event]
    processed_ids = await pipeline.process_events(events, "test_source")
    
    # Verify raw events were stored
    assert pipeline.storage.store_batch_events.call_count == 3  # Raw + DynamoDB + S3
    
    # Get the calls
    calls = pipeline.storage.store_batch_events.call_args_list
    
    # First call should be for raw events
    raw_call = calls[0]
    assert raw_call.kwargs['is_raw'] is True
    assert 'raw_ingestion_time' in raw_call.args[0][0]
    assert raw_call.args[0][0]['source'] == 'test_source'
    
    # Second call should be for DynamoDB
    dynamo_call = calls[1]
    assert 'is_raw' not in dynamo_call.kwargs
    assert 'processed_at' in dynamo_call.args[0][0]
    assert 'price_info' in dynamo_call.args[0][0]
    
    # Third call should be for S3
    s3_call = calls[2]
    assert s3_call.kwargs['is_raw'] is False
    assert 'processed_at' in s3_call.args[0][0]
    assert 'price_info' in s3_call.args[0][0]

@pytest.mark.asyncio
async def test_process_event_validation(pipeline):
    """Test event validation and standardization"""
    # Test event with missing fields
    invalid_event = {
        "title": "Invalid Event"
    }
    processed = pipeline._process_event(invalid_event)
    assert processed["title"] == "Invalid Event"
    assert "start_datetime" not in processed
    assert "location" not in processed
    
    # Test event with invalid date format
    invalid_date_event = {
        "title": "Invalid Date Event",
        "start_time": "invalid_date"
    }
    processed = pipeline._process_event(invalid_date_event)
    assert processed["title"] == "Invalid Date Event"
    assert "start_datetime" not in processed

@pytest.mark.asyncio
async def test_price_tier_calculation(pipeline):
    """Test price tier calculation"""
    assert pipeline._calculate_price_tier(0) == "free"
    assert pipeline._calculate_price_tier(15) == "budget"
    assert pipeline._calculate_price_tier(35) == "medium"
    assert pipeline._calculate_price_tier(75) == "premium"
    assert pipeline._calculate_price_tier(150) == "luxury"

@pytest.mark.asyncio
async def test_batch_processing(pipeline, sample_event):
    """Test processing events in batches"""
    # Create multiple events
    events = [
        {**sample_event, "event_id": f"test{i}"}
        for i in range(30)  # More than batch_size
    ]
    
    # Setup mock responses for each batch
    def store_batch_events(batch_events, **kwargs):
        return [event['event_id'] for event in batch_events]
    
    pipeline.storage.store_batch_events = AsyncMock(side_effect=store_batch_events)
    
    # Process events
    processed_ids = await pipeline.process_events(events, "test_source")
    
    # Verify batch processing
    assert pipeline.storage.store_batch_events.call_count == 6  # 2 batches * 3 (raw + dynamo + s3)
    
    # Get the calls
    calls = pipeline.storage.store_batch_events.call_args_list
    
    # Check batch sizes
    for i in range(0, 6, 3):  # Check each batch (raw + dynamo + s3)
        raw_call = calls[i]
        dynamo_call = calls[i + 1]
        s3_call = calls[i + 2]
        
        # Verify raw storage
        assert raw_call.kwargs['is_raw'] is True
        assert len(raw_call.args[0]) <= pipeline.batch_size
        
        # Verify DynamoDB storage
        assert 'is_raw' not in dynamo_call.kwargs
        assert len(dynamo_call.args[0]) <= pipeline.batch_size
        
        # Verify S3 storage
        assert s3_call.kwargs['is_raw'] is False
        assert len(s3_call.args[0]) <= pipeline.batch_size
    
    # Verify total processed events
    assert len(processed_ids) == 30
    assert all(f"test{i}" in processed_ids for i in range(30))

@pytest.mark.asyncio
async def test_error_handling(pipeline, sample_event):
    """Test error handling during processing"""
    # Setup storage to fail
    pipeline.storage.store_batch_events.side_effect = Exception("Storage error")
    
    # Process events
    processed_ids = await pipeline.process_events([sample_event], "test_source")
    
    # Verify error handling
    assert processed_ids == []
    
@pytest.mark.asyncio
async def test_reprocess_raw_events(pipeline, sample_event):
    """Test reprocessing raw events"""
    # Setup mock responses
    pipeline.storage.list_raw_events = AsyncMock(return_value=[{"event_id": "test123"}])
    pipeline.storage.get_raw_event = AsyncMock(return_value=sample_event)
    pipeline.storage.store_processed_event = AsyncMock(return_value="test123")
    
    # Reprocess events
    processed_ids = await pipeline.reprocess_raw_events(
        "test_source",
        start_date=datetime.utcnow(),
        end_date=datetime.utcnow()
    )
    
    # Verify reprocessing
    assert processed_ids == ["test123"]
    
    # Verify storage calls
    pipeline.storage.list_raw_events.assert_called_once()
    pipeline.storage.get_raw_event.assert_called_once_with("test123")
    pipeline.storage.store_processed_event.assert_called_once() 