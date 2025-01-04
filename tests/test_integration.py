import pytest
from datetime import datetime, timedelta
from app.utils.integration import EventIntegration
from app.utils.schema import Event, Location, PriceInfo, SourceInfo, EventAttributes

@pytest.fixture
def integration():
    """Create an EventIntegration instance for testing"""
    return EventIntegration()

@pytest.mark.asyncio
async def test_collect_and_process_events(integration):
    """Test collecting and processing events from all sources"""
    # Set up test parameters
    location = "San Francisco, CA"
    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=7)
    categories = ["music", "technology"]
    
    # Collect and process events
    events = await integration.collect_and_process_events(
        location=location,
        start_date=start_date,
        end_date=end_date,
        categories=categories
    )
    
    # Verify we got some events
    assert len(events) > 0
    
    # Verify event structure
    for event in events:
        assert isinstance(event, Event)
        assert event.event_id
        assert event.title
        assert event.description
        assert event.start_datetime >= start_date
        assert event.start_datetime <= end_date
        assert event.location.city.lower() == "san francisco"
        assert event.location.state.upper() == "CA"
        assert event.price_info.currency == "USD"
        assert event.source.platform in ["meta", "eventbrite", "scraper"]
        assert event.extracted_topics
        assert event.sentiment_scores["positive"] >= 0
        assert event.sentiment_scores["negative"] >= 0
        assert event.sentiment_scores["positive"] + event.sentiment_scores["negative"] == 1.0

@pytest.mark.asyncio
async def test_get_processed_events(integration):
    """Test retrieving processed events from storage"""
    # Set up test parameters
    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=7)
    
    # Get processed events
    events = await integration.get_processed_events(
        start_date=start_date,
        end_date=end_date
    )
    
    # Verify event structure
    for event in events:
        assert isinstance(event, Event)
        assert event.event_id
        assert event.title
        assert event.description
        assert event.start_datetime >= start_date
        assert event.start_datetime <= end_date
        assert event.price_info.currency == "USD"
        assert event.source.platform in ["meta", "eventbrite", "scraper"]
        assert event.extracted_topics
        assert event.sentiment_scores["positive"] >= 0
        assert event.sentiment_scores["negative"] >= 0
        assert event.sentiment_scores["positive"] + event.sentiment_scores["negative"] == 1.0

@pytest.mark.asyncio
async def test_error_handling(integration):
    """Test error handling in event collection and processing"""
    # Test with invalid location
    events = await integration.collect_and_process_events(
        location="Invalid Location",
        start_date=datetime.utcnow(),
        end_date=datetime.utcnow() + timedelta(days=1)
    )
    
    # Should return empty list on error, not raise exception
    assert isinstance(events, list)
    
    # Test with invalid date range
    events = await integration.collect_and_process_events(
        location="San Francisco, CA",
        start_date=datetime.utcnow() + timedelta(days=7),
        end_date=datetime.utcnow()  # End date before start date
    )
    
    # Should return empty list on error, not raise exception
    assert isinstance(events, list) 