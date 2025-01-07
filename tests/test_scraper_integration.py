import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from sqlalchemy.orm import Session
from app.services.event_collection import EventCollectionService
from app.models.event import Event
from app.db.session import SessionLocal
import logging
import os

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def cleanup_db():
    """Clean up the database before each test"""
    db = SessionLocal()
    try:
        db.query(Event).delete()
        db.commit()
    finally:
        db.close()

@pytest.fixture
def db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@pytest.fixture
def mock_scrapfly_key():
    """Mock Scrapfly API key for testing"""
    with patch.dict(os.environ, {'SCRAPFLY_API_KEY': 'test-key'}):
        yield 'test-key'

@pytest.fixture
def mock_event_responses():
    """Mock event responses for different platforms"""
    base_event = {
        'title': 'Test Event',
        'description': 'Test Description',
        'start_datetime': '2024-01-01T10:00:00Z',
        'end_datetime': '2024-01-01T12:00:00Z',
        'location': {
            'venue_name': 'Test Venue',
            'address': '123 Test St',
            'lat': 37.7749,
            'lng': -122.4194
        },
        'categories': ['Music', 'Arts'],
        'price_info': {
            'min': 10.0,
            'max': 50.0,
            'currency': 'USD'
        },
        'images': ['https://example.com/image.jpg'],
        'tags': ['test', 'music']
    }

    return {
        'eventbrite': {
            **base_event,
            'source': {'platform': 'eventbrite', 'url': 'https://eventbrite.com/test'},
            'event_id': 'eb-test-1'
        },
        'meetup': {
            **base_event,
            'source': {'platform': 'meetup', 'url': 'https://meetup.com/test'},
            'event_id': 'mu-test-1'
        },
        'facebook': {
            **base_event,
            'source': {'platform': 'facebook', 'url': 'https://facebook.com/events/test'},
            'event_id': 'fb-test-1'
        }
    }

@pytest.mark.asyncio
async def test_scraper_initialization(mock_scrapfly_key):
    """Test that scrapers are properly initialized"""
    service = EventCollectionService()
    
    # Verify scrapers were initialized
    assert len(service.scrapers) > 0, "No scrapers were initialized"
    
    # Verify only active scrapers are present
    platforms = [scraper.platform for scraper in service.scrapers]
    assert all(platform in ['eventbrite', 'meetup', 'facebook'] for platform in platforms)
    assert 'ticketmaster' not in platforms
    assert 'stubhub' not in platforms

@pytest.mark.asyncio
async def test_event_collection_integration(db: Session, mock_scrapfly_key, mock_event_responses):
    """Test that all active scrapers can successfully scrape and store events"""
    service = EventCollectionService()

    # Test location
    test_location = {
        'city': 'San Francisco',
        'state': 'California',
        'latitude': 37.7749,
        'longitude': -122.4194
    }

    # Test date range (next 7 days)
    date_range = {
        'start': datetime.utcnow(),
        'end': datetime.utcnow() + timedelta(days=7)
    }

    # Mock active scrapers
    patches = [
        patch('app.scrapers.eventbrite_scrapfly.EventbriteScrapflyScraper.scrape_events', 
              return_value=[mock_event_responses['eventbrite']]),
        patch('app.scrapers.meetup_scrapfly.MeetupScrapflyScraper.scrape_events', 
              return_value=[mock_event_responses['meetup']]),
        patch('app.scrapers.facebook_scrapfly.FacebookScrapflyScraper.scrape_events', 
              return_value=[mock_event_responses['facebook']])
    ]

    # Apply all patches
    for p in patches:
        p.start()

    try:
        # Collect events
        logger.info("Starting event collection...")
        new_events_count = await service.collect_events([test_location], date_range)
        logger.info(f"Collected {new_events_count} new events")

        # Verify events were stored
        assert new_events_count == 3, "Expected 3 events (1 from each active scraper)"

        # Verify events from each platform
        for platform in ['eventbrite', 'meetup', 'facebook']:
            event = db.query(Event).filter(Event.source == platform).first()
            assert event is not None, f"No event found for {platform}"
            assert event.title == 'Test Event'
            assert event.source == platform
            assert event.source_id == mock_event_responses[platform]['event_id']
            
            # Verify location data
            assert event.venue['name'] == 'Test Venue'
            assert event.venue['address'] == '123 Test St'
            
            # Verify categories and tags
            assert 'Music' in event.categories
            assert all(tag in event.tags for tag in ['test', 'music'])
    finally:
        # Stop all patches
        for p in patches:
            p.stop()

@pytest.mark.asyncio
async def test_event_collection_error_handling(db: Session, mock_scrapfly_key):
    """Test error handling during event collection"""
    service = EventCollectionService()

    test_location = {
        'city': 'San Francisco',
        'state': 'California',
        'latitude': 37.7749,
        'longitude': -122.4194
    }

    date_range = {
        'start': datetime.utcnow(),
        'end': datetime.utcnow() + timedelta(days=7)
    }

    # Mock scrapers with various error conditions
    patches = [
        patch('app.scrapers.eventbrite_scrapfly.EventbriteScrapflyScraper.scrape_events', 
              side_effect=Exception("API rate limit exceeded")),
        patch('app.scrapers.meetup_scrapfly.MeetupScrapflyScraper.scrape_events', 
              return_value=[]),  # Empty response
        patch('app.scrapers.facebook_scrapfly.FacebookScrapflyScraper.scrape_events', 
              side_effect=ValueError("Invalid response format"))
    ]

    for p in patches:
        p.start()

    try:
        # Service should continue running despite errors
        new_events_count = await service.collect_events([test_location], date_range)
        
        # We expect 0 events due to errors/empty responses
        assert new_events_count == 0
        
        # Verify no events were stored
        events = db.query(Event).all()
        assert len(events) == 0
    finally:
        for p in patches:
            p.stop() 

@pytest.mark.asyncio
async def test_event_data_mapping(db: Session, mock_scrapfly_key):
    """Test that event data is properly mapped and stored"""
    service = EventCollectionService()

    # Test event with string datetime and various field formats
    test_event = {
        'title': 'Test Event',
        'description': 'Test Description',
        'start_datetime': '2024-01-01T10:00:00Z',
        'end_datetime': '2024-01-01T12:00:00Z',
        'location': {
            'venue_name': 'Test Venue',
            'address': '123 Test St',
            'lat': 37.7749,
            'lng': -122.4194
        },
        'categories': ['Music', 'Arts'],
        'price_info': {
            'min': 10.0,
            'max': 50.0,
            'currency': 'USD'
        },
        'source': {
            'platform': 'eventbrite',
            'url': 'https://eventbrite.com/test'
        },
        'event_id': 'test-123',
        'images': ['https://example.com/image.jpg'],
        'tags': ['test', 'music']
    }

    # Mock the scraper to return our test event
    with patch('app.scrapers.eventbrite_scrapfly.EventbriteScrapflyScraper.scrape_events', 
               return_value=[test_event]):
        
        # Test location
        test_location = {
            'city': 'San Francisco',
            'state': 'California',
            'latitude': 37.7749,
            'longitude': -122.4194
        }

        # Test date range
        date_range = {
            'start': datetime.utcnow(),
            'end': datetime.utcnow() + timedelta(days=7)
        }

        # Collect and store the event
        new_events_count = await service.collect_events([test_location], date_range)
        assert new_events_count == 1, "Expected 1 event to be stored"

        # Retrieve and verify the stored event
        stored_event = db.query(Event).filter(
            Event.source == 'eventbrite',
            Event.source_id == 'test-123'
        ).first()

        assert stored_event is not None, "Event was not stored"
        
        # Verify datetime parsing
        assert isinstance(stored_event.start_time, datetime)
        assert isinstance(stored_event.end_time, datetime)
        assert stored_event.start_time.isoformat() + 'Z' == '2024-01-01T10:00:00Z'
        assert stored_event.end_time.isoformat() + 'Z' == '2024-01-01T12:00:00Z'

        # Verify venue data
        assert stored_event.venue == {
            'name': 'Test Venue',
            'address': '123 Test St',
            'latitude': 37.7749,
            'longitude': -122.4194
        }

        # Verify other fields
        assert stored_event.title == 'Test Event'
        assert stored_event.description == 'Test Description'
        assert stored_event.categories == ['Music', 'Arts']
        assert stored_event.price_info == {'min': 10.0, 'max': 50.0, 'currency': 'USD'}
        assert stored_event.url == 'https://eventbrite.com/test'
        assert stored_event.image_url == 'https://example.com/image.jpg'
        assert stored_event.tags == ['test', 'music']
        assert stored_event.source == 'eventbrite'
        assert stored_event.source_id == 'test-123'

        # Verify timestamps
        assert stored_event.created_at is not None
        assert stored_event.updated_at is not None 