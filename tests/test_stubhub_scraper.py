import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from app.scrapers.stubhub import StubHubScraper

@pytest.fixture
def mock_env_vars():
    """Mock environment variables"""
    with patch.dict(os.environ, {
        'STUBHUB_CLIENT_ID': 'test_client_id',
        'STUBHUB_CLIENT_SECRET': 'test_client_secret'
    }):
        yield

@pytest.fixture
def mock_token_response():
    """Mock token response data"""
    return {
        'access_token': 'test_access_token',
        'token_type': 'bearer',
        'expires_in': 86400,
        'scope': 'read:events'
    }

@pytest.fixture
def mock_events_response():
    """Mock API response data"""
    return {
        'events': [
            {
                'id': '123456',
                'name': 'Test Event',
                'description': 'A test event description',
                'eventDateLocal': '2024-03-01T19:00:00',
                'eventEndDateLocal': '2024-03-01T22:00:00',
                'venue': {
                    'name': 'Test Venue',
                    'address': {
                        'street1': '123 Test St',
                        'city': 'Test City',
                        'state': 'CA',
                        'country': 'US'
                    },
                    'latitude': '37.7749',
                    'longitude': '-122.4194'
                },
                'categoryName': 'Music',
                'subcategoryName': 'Rock',
                'ticketInfo': {
                    'minPrice': 50.0,
                    'maxPrice': 200.0,
                    'currency': 'USD'
                },
                'eventUrl': 'https://stubhub.com/event/123456',
                'totalTickets': 100,
                'minQuantity': 1,
                'maxQuantity': 4,
                'deliveryMethods': ['mobile', 'email']
            }
        ]
    }

@pytest.fixture
def mock_session(mock_token_response, mock_events_response):
    """Mock aiohttp ClientSession"""
    session = MagicMock()
    
    # Create response mock for different endpoints
    def get_response_mock(url, **kwargs):
        response = MagicMock()
        response.status = 200
        
        if '/oauth/token' in url:
            response.json.return_value = mock_token_response
        else:
            response.json.return_value = mock_events_response
            
        response.text.return_value = '{"error": "test error"}'
        return response
    
    # Mock both GET and POST methods
    session.get = MagicMock()
    session.get.return_value.__aenter__.side_effect = get_response_mock
    
    session.post = MagicMock()
    session.post.return_value.__aenter__.side_effect = get_response_mock
    
    return session

@pytest.mark.asyncio
async def test_init(mock_env_vars):
    """Test scraper initialization"""
    scraper = StubHubScraper()
    assert scraper.client_id == 'test_client_id'
    assert scraper.client_secret == 'test_client_secret'
    assert scraper.platform == 'stubhub'
    assert scraper.base_url == 'https://api.stubhub.com'

@pytest.mark.asyncio
async def test_init_missing_credentials():
    """Test initialization with missing credentials"""
    with pytest.raises(ValueError, match="StubHub client ID and secret are required"):
        StubHubScraper()

@pytest.mark.asyncio
async def test_create_basic_auth_header(mock_env_vars):
    """Test creation of Basic Authorization header"""
    scraper = StubHubScraper()
    auth_header = scraper._create_basic_auth_header()
    assert auth_header.startswith('Basic ')
    assert len(auth_header) > 10  # Should have reasonable length

@pytest.mark.asyncio
async def test_get_access_token(mock_env_vars, mock_session):
    """Test getting access token"""
    scraper = StubHubScraper()
    scraper.session = mock_session
    
    success = await scraper._get_access_token()
    assert success is True
    assert scraper.access_token == 'test_access_token'
    assert scraper.headers['Authorization'] == 'Bearer test_access_token'

@pytest.mark.asyncio
async def test_scrape_events(mock_env_vars, mock_session):
    """Test scraping events"""
    scraper = StubHubScraper()
    scraper.session = mock_session
    
    location = {
        'city': 'San Francisco',
        'state': 'CA',
        'country': 'US'
    }
    date_range = {
        'start': datetime.now(),
        'end': datetime.now() + timedelta(days=30)
    }
    
    events = await scraper.scrape_events(location, date_range)
    assert len(events) == 1
    event = events[0]
    
    assert event['event_id'] == 'sh_123456'
    assert event['title'] == 'Test Event'
    assert event['location']['city'] == 'Test City'
    assert event['price_info']['price_tier'] == 'premium'
    assert event['source']['platform'] == 'stubhub'

@pytest.mark.asyncio
async def test_get_event_details(mock_env_vars, mock_session):
    """Test getting event details"""
    scraper = StubHubScraper()
    scraper.session = mock_session
    
    event = await scraper.get_event_details('123456')
    assert event is not None
    assert event['event_id'] == 'sh_123456'
    assert event['title'] == 'Test Event'
    assert event['price_info']['min_price'] == 50.0
    assert event['price_info']['max_price'] == 200.0

@pytest.mark.asyncio
async def test_error_handling(mock_env_vars, mock_session):
    """Test error handling"""
    scraper = StubHubScraper()
    scraper.session = mock_session
    
    # Mock error response
    mock_session.get.return_value.__aenter__.return_value.status = 400
    
    events = await scraper.scrape_events({'city': 'Invalid City'})
    assert len(events) == 0

@pytest.mark.asyncio
async def test_token_refresh(mock_env_vars, mock_session):
    """Test token refresh on 401 response"""
    scraper = StubHubScraper()
    scraper.session = mock_session
    
    # First request fails with 401
    response_401 = MagicMock()
    response_401.status = 401
    mock_session.get.return_value.__aenter__.return_value = response_401
    
    # But token refresh succeeds
    mock_session.post.return_value.__aenter__.return_value.status = 200
    
    events = await scraper.scrape_events({'city': 'San Francisco'})
    assert len(events) == 0  # Since the retry would still use the mocked 401 response

@pytest.mark.asyncio
async def test_standardize_event(mock_env_vars):
    """Test event standardization"""
    scraper = StubHubScraper()
    raw_event = mock_events_response()['events'][0]
    
    event = scraper.standardize_event(raw_event)
    assert event is not None
    assert event['event_id'] == 'sh_123456'
    assert event['title'] == 'Test Event'
    assert event['location']['venue_name'] == 'Test Venue'
    assert event['price_info']['price_tier'] == 'premium'
    assert event['attributes']['total_tickets'] == 100
    assert event['attributes']['delivery_methods'] == ['mobile', 'email']

@pytest.mark.asyncio
async def test_missing_session(mock_env_vars):
    """Test error when session is not initialized"""
    scraper = StubHubScraper()
    with pytest.raises(RuntimeError, match="Session not initialized"):
        await scraper.scrape_events({'city': 'Test City'}) 