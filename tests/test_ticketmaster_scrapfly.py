import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from app.scrapers.ticketmaster_scrapfly import TicketmasterScrapflyScraper

# Test location
TEST_LOCATION = {
    'lat': 37.7749,
    'lng': -122.4194,
    'city': 'san-francisco',
    'state': 'ca',
    'radius': 10
}

# Test date range
TEST_DATE_RANGE = {
    'start': datetime.utcnow(),
    'end': datetime.utcnow() + timedelta(days=7)
}

@pytest.fixture
def mock_scrapfly_client():
    """Mock ScrapflyClient"""
    with patch('scrapfly.ScrapflyClient', autospec=True) as mock:
        # Create a mock instance that will be returned
        mock_instance = AsyncMock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_ticketmaster_html():
    """Mock Ticketmaster HTML response"""
    return """
        <div data-testid="event-tile">
            <h1 data-testid="event-title">Test Ticketmaster Event</h1>
            <div data-testid="event-description">A test event description</div>
            <a data-testid="event-link" href="/event/test-event-123">Event Link</a>
            <time data-testid="event-datetime" datetime="2024-03-01T19:00:00Z">March 1, 2024</time>
            <h2 data-testid="venue-name">Test Venue</h2>
            <div data-testid="venue-address">123 Test St, San Francisco, CA</div>
            <div data-testid="ticket-price">$100</div>
            <img data-testid="event-image" src="test.jpg" />
            <a data-testid="event-category">Concert</a>
            <span data-testid="event-tag">Music</span>
        </div>
    """

@pytest.mark.asyncio
async def test_ticketmaster_scraper_init():
    """Test TicketmasterScrapflyScraper initialization"""
    scraper = TicketmasterScrapflyScraper("test_key")
    assert scraper.platform == "ticketmaster"
    assert scraper.client is not None

@pytest.mark.asyncio
async def test_ticketmaster_scrape_events(mock_scrapfly_client, mock_ticketmaster_html):
    """Test Ticketmaster event scraping"""
    # Mock successful responses
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.content = mock_ticketmaster_html
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = TicketmasterScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 1
    event = events[0]
    
    # Verify event data structure
    assert event['title'] == "Test Ticketmaster Event"
    assert event['description'] == "A test event description"
    assert event['event_id'] == "test-event-123"
    assert event['source']['platform'] == "ticketmaster"
    assert event['source']['url'].endswith("/event/test-event-123")
    assert event['location']['venue_name'] == "Test Venue"
    assert event['location']['address'] == "123 Test St, San Francisco, CA"
    assert event['price_info']['is_free'] is False
    assert event['price_info']['amount'] == 100.0
    assert event['images'] == ["test.jpg"]
    assert "Concert" in event['categories']
    assert "Music" in event['tags']

@pytest.mark.asyncio
async def test_ticketmaster_error_handling(mock_scrapfly_client):
    """Test Ticketmaster error handling"""
    # Mock failed response
    mock_result = MagicMock()
    mock_result.success = False
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = TicketmasterScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 0

@pytest.mark.asyncio
async def test_ticketmaster_html_parsing():
    """Test Ticketmaster HTML parsing functions"""
    scraper = TicketmasterScrapflyScraper("test_key")
    soup = BeautifulSoup("""
        <div>
            <h1 data-testid="event-title">Test Event</h1>
            <div data-testid="event-description">Test description</div>
            <time data-testid="event-datetime" datetime="2024-03-01T19:00:00Z">March 1</time>
            <h2 data-testid="venue-name">Test Venue</h2>
            <div data-testid="venue-address">Test Address</div>
            <a data-testid="event-category">Sports</a>
            <div data-testid="ticket-price">$150</div>
            <img data-testid="event-image" src="test.jpg" />
            <span data-testid="event-tag">Basketball</span>
        </div>
    """, 'html.parser')
    
    assert scraper._extract_title(soup) == "Test Event"
    assert scraper._extract_description(soup) == "Test description"
    # Compare with timezone-aware datetime
    expected_dt = datetime(2024, 3, 1, 19, 0, tzinfo=timezone.utc)
    assert scraper._extract_datetime(soup) == expected_dt
    assert scraper._extract_venue_name(soup) == "Test Venue"
    assert scraper._extract_venue_address(soup) == "Test Address"
    assert scraper._extract_categories(soup) == ["Sports"]
    assert scraper._extract_price_info(soup) == {
        'is_free': False,
        'currency': 'USD',
        'amount': 150.0
    }
    assert scraper._extract_images(soup) == ["test.jpg"]
    assert scraper._extract_tags(soup) == ["Basketball"]

@pytest.mark.asyncio
async def test_ticketmaster_get_event_details(mock_scrapfly_client, mock_ticketmaster_html):
    """Test getting detailed event information"""
    # Mock successful response
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.content = mock_ticketmaster_html
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = TicketmasterScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client
    
    event = await scraper.get_event_details("https://ticketmaster.com/event/test-event-123")
    
    assert event is not None
    assert event['title'] == "Test Ticketmaster Event"
    assert event['description'] == "A test event description"
    assert event['event_id'] == "test-event-123"
    assert event['source']['platform'] == "ticketmaster"
    assert event['location']['venue_name'] == "Test Venue"
    assert event['price_info']['amount'] == 100.0 