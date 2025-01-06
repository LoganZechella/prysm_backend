import pytest
import os
from datetime import datetime, timedelta
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.stubhub_scrapfly import StubhubScrapflyScraper

# Test location (San Francisco)
TEST_LOCATION = {
    'name': 'San Francisco',
    'lat': 37.7749,
    'lng': -122.4194,
    'radius': 10  # km
}

# Test date range (next 7 days)
TEST_DATE_RANGE = {
    'start': datetime.utcnow(),
    'end': datetime.utcnow() + timedelta(days=7)
}

@pytest.mark.asyncio
async def test_meetup_scraper():
    """Test Meetup scraper implementation"""
    scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
    assert scrapfly_key, "SCRAPFLY_API_KEY environment variable is required"
    
    scraper = MeetupScrapflyScraper(scrapfly_key)
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    # Verify we got some events
    assert len(events) > 0, "No events were scraped from Meetup"
    
    # Verify event structure
    for event in events:
        assert 'title' in event
        assert 'description' in event
        assert 'event_id' in event
        assert 'source' in event
        assert event['source']['platform'] == 'meetup'
        assert 'start_datetime' in event
        assert 'location' in event
        assert 'venue_name' in event['location']
        assert 'address' in event['location']
        
        # Print event details for manual verification
        print(f"\nMeetup Event: {event['title']}")
        print(f"Date: {event['start_datetime']}")
        print(f"Venue: {event['location']['venue_name']}")
        print(f"URL: {event['source']['url']}")

@pytest.mark.asyncio
async def test_stubhub_scraper():
    """Test StubHub scraper implementation"""
    scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
    assert scrapfly_key, "SCRAPFLY_API_KEY environment variable is required"
    
    scraper = StubhubScrapflyScraper(scrapfly_key)
    events = await scraper.scrape_events(TEST_LOCATION, TEST_DATE_RANGE)
    
    # Verify we got some events
    assert len(events) > 0, "No events were scraped from StubHub"
    
    # Verify event structure
    for event in events:
        assert 'title' in event
        assert 'event_id' in event
        assert 'source' in event
        assert event['source']['platform'] == 'stubhub'
        assert 'start_datetime' in event
        assert 'location' in event
        assert 'venue_name' in event['location']
        assert 'address' in event['location']
        
        # Print event details for manual verification
        print(f"\nStubHub Event: {event['title']}")
        print(f"Date: {event['start_datetime']}")
        print(f"Venue: {event['location']['venue_name']}")
        print(f"URL: {event['source']['url']}")

if __name__ == "__main__":
    import asyncio
    
    async def run_tests():
        print("\nTesting Meetup Scraper...")
        await test_meetup_scraper()
        
        print("\nTesting StubHub Scraper...")
        await test_stubhub_scraper()
    
    asyncio.run(run_tests()) 