"""
Tests for the EventCollectionService class.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List

from app.services.event_collection import EventCollectionService
from app.models.event import Event
from app.scrapers.base import ScrapflyBaseScraper

# Test data
TEST_LOCATION = {
    'city': 'San Francisco',
    'state': 'California',
    'latitude': 37.7749,
    'longitude': -122.4194,
    'radius': 10
}

TEST_EVENT_DATA = {
    'title': 'Test Event',
    'description': 'Test Description',
    'start_time': datetime.utcnow(),
    'end_time': datetime.utcnow() + timedelta(hours=2),
    'location': {
        'venue_name': 'Test Venue',
        'address': '123 Test St'
    },
    'categories': ['test'],
    'price_info': {'min': 0, 'max': 100},
    'source': 'test_platform',
    'event_id': 'test123',
    'url': 'https://test.com/event',
    'image_url': 'https://test.com/image.jpg'
}

@pytest.fixture
def mock_scraper():
    """Create a mock scraper for testing"""
    scraper = Mock(spec=ScrapflyBaseScraper)
    scraper.platform = 'test_platform'
    scraper.scrape_events = AsyncMock(return_value=[TEST_EVENT_DATA])
    return scraper

@pytest.fixture
def event_service(mock_scraper):
    """Create an EventCollectionService instance with a mock scraper"""
    with patch.dict('os.environ', {'SCRAPFLY_API_KEY': 'test_key'}):
        service = EventCollectionService()
        service.scrapers = [mock_scraper]
        return service

@pytest.mark.asyncio
async def test_collect_events(event_service, mock_scraper):
    """Test collecting events from scrapers"""
    # Test with default parameters
    total_events = await event_service.collect_events()
    assert total_events == 1
    mock_scraper.scrape_events.assert_called_once()

    # Test with custom parameters
    date_range = {
        'start': datetime.utcnow(),
        'end': datetime.utcnow() + timedelta(days=7)
    }
    categories = ['music', 'art']
    
    mock_scraper.scrape_events.reset_mock()
    total_events = await event_service.collect_events(
        locations=[TEST_LOCATION],
        date_range=date_range,
        categories=categories
    )
    assert total_events == 1
    mock_scraper.scrape_events.assert_called_once_with(
        {'city': 'San Francisco', 'state': 'California', 'lat': 37.7749, 'lng': -122.4194, 'radius': 10},
        date_range,
        categories
    )

@pytest.mark.asyncio
async def test_store_events(event_service):
    """Test storing events in the database"""
    with patch('app.services.event_collection.SessionLocal') as mock_session:
        mock_db = Mock()
        mock_session.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None

        event_service._store_events([TEST_EVENT_DATA])
        
        # Verify that add was called with an Event object
        mock_db.add.assert_called_once()
        added_event = mock_db.add.call_args[0][0]
        assert isinstance(added_event, Event)
        assert added_event.title == TEST_EVENT_DATA['title']
        assert added_event.description == TEST_EVENT_DATA['description']
        
        # Verify commit was called
        mock_db.commit.assert_called_once()

@pytest.mark.asyncio
async def test_update_existing_event(event_service):
    """Test updating an existing event"""
    with patch('app.services.event_collection.SessionLocal') as mock_session:
        mock_db = Mock()
        mock_session.return_value = mock_db
        
        # Create an existing event
        existing_event = Event(
            title='Old Title',
            description='Old Description',
            start_time=datetime.utcnow(),
            source='test_platform',
            source_id='test123',
            url='https://test.com/old'
        )
        mock_db.query.return_value.filter.return_value.first.return_value = existing_event

        # Update with new data
        event_service._store_events([TEST_EVENT_DATA])
        
        # Verify event was updated
        assert existing_event.title == TEST_EVENT_DATA['title']
        assert existing_event.description == TEST_EVENT_DATA['description']
        assert existing_event.url == TEST_EVENT_DATA['url']
        
        # Verify commit was called
        mock_db.commit.assert_called_once()

def test_validate_event_data(event_service):
    """Test event data validation"""
    # Test valid event data
    assert event_service._validate_event_data(TEST_EVENT_DATA) is True
    
    # Test invalid event data
    invalid_event = TEST_EVENT_DATA.copy()
    del invalid_event['title']
    assert event_service._validate_event_data(invalid_event) is False
    
    invalid_event = TEST_EVENT_DATA.copy()
    invalid_event['location'] = 'not a dict'
    assert event_service._validate_event_data(invalid_event) is False
    
    invalid_event = TEST_EVENT_DATA.copy()
    del invalid_event['location']['venue_name']
    assert event_service._validate_event_data(invalid_event) is False

@pytest.mark.asyncio
async def test_collection_task(event_service):
    """Test the continuous collection task"""
    # Mock collect_events to avoid actual collection
    event_service.collect_events = AsyncMock(return_value=1)
    
    # Start collection task
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        # Set up the task to run twice then stop
        async def stop_after_two_runs():
            await asyncio.sleep(0.1)
            event_service.running = False
            
        task = asyncio.create_task(stop_after_two_runs())
        await event_service.start_collection_task(interval_hours=1)
        await task
        
        # Verify collect_events was called
        assert event_service.collect_events.called
        mock_sleep.assert_called_with(3600)  # 1 hour in seconds 