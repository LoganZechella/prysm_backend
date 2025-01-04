import pytest
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from app.scrapers.facebook import FacebookEventsScraper

# Load environment variables
load_dotenv()

@pytest.fixture
def scraper():
    """Create Facebook Events scraper instance"""
    access_token = os.getenv('META_ACCESS_TOKEN')
    return FacebookEventsScraper(access_token=access_token)

@pytest.fixture
def mock_event_post():
    """Create a mock Facebook post about an event"""
    return {
        'id': '123456789',
        'message': 'Join us for an amazing concert!\n\nDate: December 31, 2024\nTime: 8:00 PM\nVenue: The Great Hall\n\nTickets: $25',
        'created_time': '2024-01-01T12:00:00+0000',
        'attachments': {
            'data': [{
                'type': 'event',
                'title': 'New Year\'s Eve Concert',
                'description': 'Ring in the new year with live music!',
                'url': 'https://facebook.com/events/123456789'
            }]
        }
    }

@pytest.fixture
def mock_page():
    """Create a mock Facebook page"""
    return {
        'id': '987654321',
        'name': 'Local Events Page',
        'location': {
            'city': 'San Francisco',
            'state': 'CA',
            'country': 'US'
        }
    }

@pytest.mark.asyncio
async def test_is_event_post(scraper, mock_event_post):
    """Test event post detection"""
    assert scraper._is_event_post(mock_event_post) == True
    
    # Test with non-event post
    regular_post = {
        'message': 'Just a regular update about our business',
        'attachments': {'data': []}
    }
    assert scraper._is_event_post(regular_post) == False
    
    # Test with event keyword in message
    keyword_post = {
        'message': 'Looking forward to the upcoming concert!',
        'attachments': {'data': []}
    }
    assert scraper._is_event_post(keyword_post) == True
    
    # Test with event URL
    url_post = {
        'message': 'Check this out',
        'attachments': {
            'data': [{
                'type': 'link',
                'url': 'https://facebook.com/events/123'
            }]
        }
    }
    assert scraper._is_event_post(url_post) == True

@pytest.mark.asyncio
async def test_extract_event_from_post(scraper, mock_event_post, mock_page):
    """Test event information extraction from post"""
    async with scraper:
        event = await scraper._extract_event_from_post(mock_event_post, mock_page)
        
        assert event is not None
        assert event['event_id'] == f"fb_{mock_event_post['id']}"
        assert event['title'] == mock_event_post['attachments']['data'][0]['title']
        assert event['description'] == mock_event_post['attachments']['data'][0]['description']
        assert event['location']['venue_name'] == mock_page['name']
        
        # Test with post without explicit event attachment
        post_without_attachment = {
            'id': '123456789',
            'message': 'Community Workshop\n\nJoin us for a day of learning!',
            'attachments': {'data': []}
        }
        event = await scraper._extract_event_from_post(post_without_attachment, mock_page)
        assert event is not None
        assert event['title'] == 'Community Workshop'

@pytest.mark.asyncio
async def test_scrape_events(scraper):
    """Test scraping events for a location"""
    location = {
        "city": "San Francisco",
        "state": "CA",
        "country": "US"
    }
    
    date_range = {
        "start": datetime.utcnow(),
        "end": datetime.utcnow() + timedelta(days=30)
    }
    
    async with scraper:
        events = await scraper.scrape_events(location, date_range)
        
        # Since we're using real API calls, we can't guarantee events will be found
        # Just verify the structure of any events that are found
        for event in events:
            assert "event_id" in event
            assert event["event_id"].startswith("fb_")
            assert "title" in event
            assert "description" in event
            assert "location" in event
            assert "source" in event
            assert event["source"]["platform"] == "facebook"
            assert "attributes" in event

@pytest.mark.asyncio
async def test_error_handling(scraper):
    """Test error handling for invalid requests"""
    async with scraper:
        # Test with invalid location
        events = await scraper.scrape_events({"city": "NonexistentCity123"})
        assert len(events) == 0
        
        # Test with invalid post ID
        event = await scraper.get_event_details("invalid_id")
        assert event is None

@pytest.mark.asyncio
async def test_date_range_filtering(scraper, mock_event_post):
    """Test date range filtering"""
    # Test event within range
    date_range = {
        "start": datetime.utcnow() - timedelta(days=1),
        "end": datetime.utcnow() + timedelta(days=365)
    }
    
    event = {
        "start_datetime": datetime.utcnow().isoformat()
    }
    assert scraper._matches_date_range(event, date_range) == True
    
    # Test event outside range
    event = {
        "start_datetime": (datetime.utcnow() + timedelta(days=366)).isoformat()
    }
    assert scraper._matches_date_range(event, date_range) == False
    
    # Test with missing date
    event = {}
    assert scraper._matches_date_range(event, date_range) == True 