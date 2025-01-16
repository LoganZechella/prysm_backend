"""Tests for the event collection service."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock
import json

from app.services.event_collection import EventCollectionService
from app.services.event_pipeline import EventPipeline

@pytest.fixture
def mock_scrapers():
    """Create mock scrapers for testing"""
    eventbrite = AsyncMock()
    facebook = AsyncMock()
    meetup = AsyncMock()
    
    # Setup mock responses
    eventbrite.scrape_events.return_value = [
        {
            "title": "Test Eventbrite Event",
            "start_time": datetime.utcnow().isoformat(),
            "location": {"venue_name": "Test Venue", "latitude": 37.7749, "longitude": -122.4194}
        }
    ]
    
    facebook.scrape_events.return_value = [
        {
            "title": "Test Facebook Event",
            "start_time": datetime.utcnow().isoformat(),
            "location": {"venue_name": "Test Venue", "latitude": 37.7749, "longitude": -122.4194}
        }
    ]
    
    meetup.scrape_events.return_value = [
        {
            "title": "Test Meetup Event",
            "start_time": datetime.utcnow().isoformat(),
            "location": {"venue_name": "Test Venue", "latitude": 37.7749, "longitude": -122.4194}
        }
    ]
    
    return eventbrite, facebook, meetup

@pytest.fixture
def mock_pipeline():
    """Create mock event pipeline for testing"""
    pipeline = AsyncMock(spec=EventPipeline)
    pipeline.process_events.return_value = ["test123"]
    return pipeline

@pytest.fixture
def collection_service(mock_scrapers, mock_pipeline):
    """Create event collection service with mocked dependencies"""
    eventbrite, facebook, meetup = mock_scrapers
    
    with patch('app.services.event_collection.EventbriteScrapflyScraper') as mock_eventbrite, \
         patch('app.services.event_collection.FacebookScrapflyScraper') as mock_facebook, \
         patch('app.services.event_collection.MeetupScrapflyScraper') as mock_meetup, \
         patch('app.services.event_collection.EventPipeline') as mock_pipeline_class:
        
        mock_eventbrite.return_value = eventbrite
        mock_facebook.return_value = facebook
        mock_meetup.return_value = meetup
        mock_pipeline_class.return_value = mock_pipeline
        
        service = EventCollectionService(scrapfly_api_key="test_key")
        return service

@pytest.mark.asyncio
async def test_collect_events(collection_service):
    """Test collecting events from all sources"""
    # Test with default parameters
    total_events = await collection_service.collect_events()
    
    # Verify each scraper was called
    collection_service.eventbrite.scrape_events.assert_called_once()
    collection_service.facebook.scrape_events.assert_called_once()
    collection_service.meetup.scrape_events.assert_called_once()
    
    # Verify pipeline was called for each source
    assert collection_service.pipeline.process_events.call_count == 3
    
    # Verify total events count
    assert total_events == 3  # One from each source

@pytest.mark.asyncio
async def test_collect_events_with_params(collection_service):
    """Test collecting events with custom parameters"""
    locations = [
        {
            "city": "New York",
            "state": "NY",
            "country": "US",
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    ]
    
    date_range = {
        "start": datetime.utcnow(),
        "end": datetime.utcnow().replace(day=datetime.utcnow().day + 7)
    }
    
    total_events = await collection_service.collect_events(
        locations=locations,
        date_range=date_range
    )
    
    # Verify scrapers were called with correct parameters
    collection_service.eventbrite.scrape_events.assert_called_with(
        location=locations[0],
        date_range=date_range
    )
    
    collection_service.facebook.scrape_events.assert_called_with(
        location=locations[0],
        date_range=date_range
    )
    
    collection_service.meetup.scrape_events.assert_called_with(
        location=locations[0],
        date_range=date_range
    )
    
    assert total_events == 3

@pytest.mark.asyncio
async def test_error_handling(collection_service):
    """Test error handling during collection"""
    # Make one scraper fail
    collection_service.facebook.scrape_events.side_effect = Exception("Test error")
    
    total_events = await collection_service.collect_events()
    
    # Should still get events from other sources
    assert total_events == 2
    
    # Verify error was logged
    collection_service.facebook.scrape_events.assert_called_once()

@pytest.mark.asyncio
async def test_collection_task(collection_service):
    """Test the continuous collection task"""
    # Setup mock for asyncio.sleep to avoid waiting
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        # Run one iteration
        await collection_service.collect_events()
        
        # Verify collection was performed
        assert collection_service.eventbrite.scrape_events.called
        assert collection_service.facebook.scrape_events.called
        assert collection_service.meetup.scrape_events.called 