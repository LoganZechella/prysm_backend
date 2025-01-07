import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.stubhub_scrapfly import StubhubScrapflyScraper

# Test location
TEST_LOCATION = {
    'lat': 37.7749,
    'lng': -122.4194,
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
def mock_meetup_html():
    """Mock Meetup HTML response"""
    return """
        <div data-element-name="eventCard">
            <h2 data-element-name="eventCardTitle">Test Meetup Event</h2>
            <p data-element-name="eventCardDescription">A test event description</p>
            <a data-element-name="eventCardLink" href="https://meetup.com/events/123">Event Link</a>
            <time data-element-name="eventDateTime" datetime="2024-03-01T19:00:00Z">March 1, 2024</time>
            <div data-element-name="venueDisplay">
                <div data-element-name="venueName">Test Venue</div>
                <div data-element-name="venueAddress">123 Test St, San Francisco, CA</div>
            </div>
            <div data-element-name="eventPrice">$25</div>
            <img data-element-name="eventImage" src="test.jpg" />
            <a data-element-name="groupCategory">Technology</a>
            <a data-element-name="groupTopic">Programming</a>
        </div>
    """

@pytest.fixture
def mock_stubhub_html():
    """Mock StubHub HTML response"""
    return """
        <div data-testid="event-card">
            <h3>Test StubHub Event</h3>
            <a href="/event/123">Event Link</a>
            <div data-testid="event-description">A test event description</div>
            <time data-testid="event-date" datetime="2024-03-01T19:00:00Z">March 1, 2024</time>
            <h2 data-testid="venue-name">Test Venue</h2>
            <div data-testid="venue-address">123 Test St, San Francisco, CA</div>
            <div data-testid="ticket-price">$50</div>
            <img data-testid="event-image" src="test.jpg" />
            <a data-testid="event-category">Concert</a>
            <span data-testid="event-tag">Rock</span>
        </div>
    """

@pytest.mark.asyncio
async def test_meetup_scraper_init():
    """Test MeetupScrapflyScraper initialization"""
    scraper = MeetupScrapflyScraper("test_key")
    assert scraper.platform == "meetup"
    assert scraper.client is not None

@pytest.mark.asyncio
async def test_meetup_scrape_events(mock_scrapfly_client, mock_meetup_html):
    """Test Meetup event scraping"""
    # Mock successful responses for both search and event detail pages
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.content = mock_meetup_html
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = MeetupScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 1
    event = events[0]
    
    # Verify event data structure
    assert event['title'] == "Test Meetup Event"
    assert event['description'] == "A test event description"
    assert event['event_id'] == "123"
    assert event['source']['platform'] == "meetup"
    assert event['source']['url'] == "https://meetup.com/events/123"
    assert event['location']['venue_name'] == "Test Venue"
    assert event['location']['address'] == "123 Test St, San Francisco, CA"
    assert event['price_info']['is_free'] is False
    assert event['price_info']['amount'] == 25.0
    assert event['images'] == ["test.jpg"]
    assert "Technology" in event['categories']
    assert "Programming" in event['tags']

@pytest.mark.asyncio
async def test_meetup_error_handling(mock_scrapfly_client):
    """Test Meetup error handling"""
    # Mock failed response
    mock_result = MagicMock()
    mock_result.success = False
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = MeetupScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 0

@pytest.mark.asyncio
async def test_stubhub_scraper_init():
    """Test StubhubScrapflyScraper initialization"""
    scraper = StubhubScrapflyScraper("test_key")
    assert scraper.platform == "stubhub"
    assert scraper.client is not None

@pytest.mark.asyncio
async def test_stubhub_scrape_events(mock_scrapfly_client, mock_stubhub_html):
    """Test StubHub event scraping"""
    # Mock successful responses
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.content = mock_stubhub_html
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = StubhubScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 1
    event = events[0]
    
    # Verify event data structure
    assert event['title'] == "Test StubHub Event"
    assert event['description'] == "A test event description"
    assert event['event_id'] == "123"
    assert event['source']['platform'] == "stubhub"
    assert event['source']['url'].endswith("/event/123")
    assert event['location']['venue_name'] == "Test Venue"
    assert event['location']['address'] == "123 Test St, San Francisco, CA"
    assert event['price_info']['is_free'] is False
    assert event['price_info']['amount'] == 50.0
    assert event['images'] == ["test.jpg"]
    assert "Concert" in event['categories']
    assert "Rock" in event['tags']

@pytest.mark.asyncio
async def test_stubhub_error_handling(mock_scrapfly_client):
    """Test StubHub error handling"""
    # Mock failed response
    mock_result = MagicMock()
    mock_result.success = False
    
    # Set up the async mock to return our mock result
    mock_scrapfly_client.async_scrape = AsyncMock(return_value=mock_result)
    
    scraper = StubhubScrapflyScraper("test_key")
    scraper.client = mock_scrapfly_client  # Directly set the mock client
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    assert len(events) == 0

@pytest.mark.asyncio
async def test_meetup_html_parsing():
    """Test Meetup HTML parsing functions"""
    scraper = MeetupScrapflyScraper("test_key")
    soup = BeautifulSoup("""
        <div>
            <a data-element-name="groupCategory">Tech</a>
            <a data-element-name="groupCategory">Social</a>
            <div data-element-name="eventPrice">Free</div>
            <img data-element-name="eventImage" src="img1.jpg" />
            <img data-element-name="eventImage" src="img2.jpg" />
            <a data-element-name="groupTopic">Python</a>
            <a data-element-name="groupTopic">Web</a>
        </div>
    """, 'html.parser')
    
    assert scraper._extract_categories(soup) == ["Tech", "Social"]
    assert scraper._extract_price_info(soup) == {'is_free': True}
    assert scraper._extract_images(soup) == ["img1.jpg", "img2.jpg"]
    assert scraper._extract_tags(soup) == ["Python", "Web"]

@pytest.mark.asyncio
async def test_stubhub_html_parsing():
    """Test StubHub HTML parsing functions"""
    scraper = StubhubScrapflyScraper("test_key")
    soup = BeautifulSoup("""
        <div>
            <div data-testid="event-description">Test description</div>
            <time data-testid="event-date" datetime="2024-03-01T19:00:00Z">March 1</time>
            <h2 data-testid="venue-name">Test Venue</h2>
            <div data-testid="venue-address">Test Address</div>
            <a data-testid="event-category">Music</a>
            <div data-testid="ticket-price">$75</div>
            <img data-testid="event-image" src="test.jpg" />
            <span data-testid="event-tag">Live</span>
        </div>
    """, 'html.parser')
    
    assert scraper._extract_description(soup) == "Test description"
    # Compare with timezone-aware datetime
    expected_dt = datetime(2024, 3, 1, 19, 0, tzinfo=timezone.utc)
    assert scraper._extract_datetime(soup) == expected_dt
    assert scraper._extract_venue_name(soup) == "Test Venue"
    assert scraper._extract_venue_address(soup) == "Test Address"
    assert scraper._extract_categories(soup) == ["Music"]
    assert scraper._extract_price_info(soup) == {
        'is_free': False,
        'currency': 'USD',
        'amount': 75.0
    }
    assert scraper._extract_images(soup) == ["test.jpg"]
    assert scraper._extract_tags(soup) == ["Live"] 