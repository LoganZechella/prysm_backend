import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import pytest_asyncio
from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper
from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper
from app.scrapers.ticketmaster_scrapfly import TicketmasterScrapflyScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.stubhub_scrapfly import StubhubScrapflyScraper
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

@pytest_asyncio.fixture
async def mock_scrapfly():
    """Mock Scrapfly API client."""
    with patch("app.scrapers.eventbrite_scrapfly.ScrapflyClient") as mock:
        mock.return_value.scrape.return_value = {
            "result": {
                "content": """
                <div class="event-card">
                    <h2>Test Event</h2>
                    <p>Test Description</p>
                    <div class="event-details">
                        <time datetime="2024-03-01T19:00:00Z">March 1, 2024 7:00 PM</time>
                        <div class="venue">Test Venue, Test City</div>
                        <div class="category">Music</div>
                        <div class="price">$10.00 - $50.00</div>
                    </div>
                </div>
                """
            }
        }
        yield mock

@pytest.mark.asyncio
async def test_eventbrite_collection(mock_scrapfly):
    """Test collecting events from Eventbrite using Scrapfly."""
    scraper = EventbriteScrapflyScraper("test-scrapfly-key")
    events = await scraper.scrape_events({
        'city': 'Test City',
        'state': 'Test State',
        'coordinates': {'lat': 37.7749, 'lng': -122.4194}
    })
    
    assert len(events) == 1
    event = events[0]
    assert event['title'] == "Test Event"
    assert event['description'] == "Test Description"
    assert event['location']['venue_name'] == "Test Venue"
    assert event['location']['coordinates']['lat'] == 37.7749
    assert event['location']['coordinates']['lng'] == -122.4194
    assert "Music" in event['categories']
    assert event['price_info'] is not None
    assert event['price_info']['min_price'] == 10.00
    assert event['price_info']['max_price'] == 50.00
    assert event['price_info']['currency'] == "USD" 