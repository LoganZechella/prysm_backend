import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper

# Test location
TEST_LOCATION = {
    'lat': 37.7749,
    'lng': -122.4194,
    'city': 'san-francisco',
    'state': 'california',
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
def mock_eventbrite_html():
    """Mock Eventbrite HTML response"""
    return """
        <div data-testid="event-card">
            <h1 data-testid="event-title">Test Eventbrite Event</h1>
            <div data-testid="event-description">A test event description</div>
            <a data-testid="event-card-link" href="https://eventbrite.com/e/test-event-123">Event Link</a>
            <div data-testid="event-card-date">Tomorrow at 7:00 PM</div>
            <time data-testid="event-start-date" datetime="2024-03-01T19:00:00Z">March 1, 2024</time>
            <time data-testid="event-end-date" datetime="2024-03-01T22:00:00Z">March 1, 2024</time>
            <h2 data-testid="venue-name">Test Venue</h2>
            <div data-testid="venue-address">123 Test St, San Francisco, CA</div>
            <div data-testid="ticket-price">$35</div>
            <img data-testid="event-image" src="test.jpg" />
            <a data-testid="event-category">Conference</a>
            <span data-testid="event-tag">Technology</span>
        </div>
    """

@pytest.mark.asyncio
async def test_eventbrite_scraper_init():
    """Test EventbriteScrapflyScraper initialization"""
    scraper = EventbriteScrapflyScraper("test_key")
    assert scraper.platform == "eventbrite"
    assert scraper.client is not None

@pytest.mark.asyncio
async def test_eventbrite_scrape_events(mock_scrapfly_client, mock_eventbrite_html):
    """Test Eventbrite event scraping"""
    # Mock successful responses
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.content = mock_eventbrite_html
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) > 0
    event = events[0]
    
    # Verify event data structure
    assert event['title'] == "Test Eventbrite Event"
    assert event['description'] == "A test event description"
    assert event['source'] == "eventbrite"
    assert event['url'].startswith("https://eventbrite.com/e/")
    assert event['venue']['name'] == "Test Venue"
    assert event['venue']['address'] == "123 Test St, San Francisco, CA"
    assert event['price_info']['amount'] == 35.0
    assert event['price_info']['currency'] == "USD"

@pytest.mark.asyncio
async def test_eventbrite_error_handling(mock_scrapfly_client):
    """Test Eventbrite error handling"""
    # Mock failed response
    mock_result = MagicMock()
    mock_result.success = False
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 0

@pytest.mark.asyncio
async def test_eventbrite_html_parsing():
    """Test Eventbrite HTML parsing functions"""
    scraper = EventbriteScrapflyScraper("test_key")
    soup = BeautifulSoup("""
        <div>
            <h1 data-testid="event-title">Test Event</h1>
            <div data-testid="event-description">Test description</div>
            <time data-testid="event-start-date" datetime="2024-03-01T19:00:00Z">March 1</time>
            <time data-testid="event-end-date" datetime="2024-03-01T22:00:00Z">March 1</time>
            <h2 data-testid="venue-name">Test Venue</h2>
            <div data-testid="venue-address">Test Address</div>
            <a data-testid="event-category">Workshop</a>
            <div data-testid="ticket-price">$75</div>
            <img data-testid="event-image" src="test.jpg" />
            <span data-testid="event-tag">Education</span>
        </div>
    """, 'html.parser')
    
    assert scraper._extract_title(soup) == "Test Event"
    assert scraper._extract_description(soup) == "Test description"
    # Compare with timezone-aware datetime
    expected_start = datetime(2024, 3, 1, 19, 0, tzinfo=timezone.utc)
    expected_end = datetime(2024, 3, 1, 22, 0, tzinfo=timezone.utc)
    assert scraper._extract_datetime(soup) == expected_start
    assert scraper._extract_end_datetime(soup) == expected_end
    assert scraper._extract_venue_name(soup) == "Test Venue"
    assert scraper._extract_venue_address(soup) == "Test Address"
    assert scraper._extract_categories(soup) == ["Workshop"]
    assert scraper._extract_price_info(soup) == {
        'is_free': False,
        'currency': 'USD',
        'amount': 75.0
    }
    assert scraper._extract_images(soup) == ["test.jpg"]
    assert scraper._extract_tags(soup) == ["Education"]

@pytest.mark.asyncio
async def test_eventbrite_get_event_details(mock_scrapfly_client, mock_eventbrite_html):
    """Test getting detailed event information"""
    # Mock successful response
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.content = mock_eventbrite_html
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client
    
    event = await scraper.get_event_details("https://eventbrite.com/e/test-event-123")
    
    assert event is not None
    assert event['title'] == "Test Eventbrite Event"
    assert event['description'] == "A test event description"
    assert event['event_id'] == "123"
    assert event['source']['platform'] == "eventbrite"
    assert event['location']['venue_name'] == "Test Venue"
    assert event['price_info']['amount'] == 35.0 

@pytest.mark.asyncio
@pytest.mark.parametrize("date_text,expected_result", [
    ("Today at 7:00 PM", lambda: datetime.now().replace(
        hour=19, minute=0, second=0, microsecond=0)),
    ("Tomorrow at 8:00 PM", lambda: (datetime.now() + timedelta(days=1)).replace(
        hour=20, minute=0, second=0, microsecond=0)),
    ("Sat, January 13, 2024 7:00 PM", 
     lambda: datetime(2024, 1, 13, 19, 0)),
])
async def test_parse_eventbrite_date(date_text, expected_result):
    """Test Eventbrite date parsing with various formats"""
    scraper = EventbriteScrapflyScraper("test_key")
    parsed_date = scraper._parse_eventbrite_date(date_text)
    expected = expected_result()
    
    # Compare only date and time components that matter
    assert parsed_date.year == expected.year
    assert parsed_date.month == expected.month
    assert parsed_date.day == expected.day
    assert parsed_date.hour == expected.hour
    assert parsed_date.minute == expected.minute

@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_date", [
    "Invalid date format",
    "2024",
    "",
    None
])
async def test_parse_eventbrite_date_errors(invalid_date):
    """Test Eventbrite date parsing error handling"""
    scraper = EventbriteScrapflyScraper("test_key")
    with pytest.raises(ValueError):
        scraper._parse_eventbrite_date(invalid_date)

@pytest.mark.asyncio
async def test_eventbrite_card_parsing_with_dates(mock_eventbrite_html):
    """Test parsing complete Eventbrite cards including date handling"""
    scraper = EventbriteScrapflyScraper("test_key")
    soup = BeautifulSoup(mock_eventbrite_html, 'html.parser')
    event_card = soup.find('div', {'data-testid': 'event-card'})
    
    event_data = scraper._parse_eventbrite_card(event_card, TEST_LOCATION)
    
    assert event_data is not None
    assert 'start_time' in event_data
    assert isinstance(datetime.fromisoformat(event_data['start_time'].replace('Z', '+00:00')), datetime) 

@pytest.mark.asyncio
async def test_scrapfly_asp_error(mock_scrapfly_client):
    """Test handling of ASP Shield errors"""
    mock_result = MagicMock()
    mock_result.success = False
    mock_result.error = {
        'code': 'ERR::ASP::SHIELD_ERROR',
        'description': 'ASP Shield error occurred'
    }
    
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client
    
    # Should return None for non-retryable error
    result = await scraper._make_request_with_retry("https://test.com", scraper.config)
    assert result is None

@pytest.mark.asyncio
async def test_scrapfly_timeout_error(mock_scrapfly_client):
    """Test handling of timeout errors with automatic timeout increase"""
    mock_result = MagicMock()
    mock_result.success = False
    mock_result.error = {
        'code': 'ERR::SCRAPE::OPERATION_TIMEOUT',
        'description': 'Operation timed out'
    }
    
    # First call times out, second succeeds
    mock_scrapfly_client.async_scrape = AsyncMock(side_effect=[
        mock_result,
        MagicMock(success=True, content="<html></html>")
    ])
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client
    initial_timeout = scraper.config['timeout']
    
    await scraper._make_request_with_retry("https://test.com", scraper.config)
    
    # Verify timeout was increased
    assert scraper.config['timeout'] > initial_timeout

@pytest.mark.asyncio
async def test_scrapfly_rate_limit_error(mock_scrapfly_client):
    """Test handling of rate limit errors with additional delay"""
    mock_result = MagicMock()
    mock_result.success = False
    mock_result.error = {
        'code': 'ERR::THROTTLE::MAX_REQUEST_RATE_EXCEEDED',
        'description': 'Rate limit exceeded'
    }
    
    # Mock async_scrape to fail with rate limit first, then succeed
    mock_scrapfly_client.async_scrape = AsyncMock(side_effect=[
        mock_result,
        MagicMock(success=True, content="<html></html>")
    ])
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client
    
    start_time = datetime.now()
    await scraper._make_request_with_retry("https://test.com", scraper.config)
    duration = (datetime.now() - start_time).total_seconds()
    
    # Verify that extra delay was added (should be > 5 seconds)
    assert duration >= 5

@pytest.mark.asyncio
async def test_max_retries_exceeded(mock_scrapfly_client):
    """Test that scraper gives up after max retries"""
    mock_result = MagicMock()
    mock_result.success = False
    mock_result.error = {
        'code': 'ERR::SCRAPE::OPERATION_TIMEOUT',
        'description': 'Operation timed out'
    }
    
    # Make it fail consistently
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = EventbriteScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client
    
    result = await scraper._make_request_with_retry("https://test.com", {'retry_attempts': 3})
    
    # Should have given up after 3 attempts
    assert mock_scrapfly_client.async_scrape.call_count == 3
    assert result is None 