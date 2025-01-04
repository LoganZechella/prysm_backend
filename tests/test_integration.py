import pytest
from datetime import datetime, timedelta
from app.utils.integration import EventIntegration
from app.utils.schema import Event, Location, PriceInfo, SourceInfo, EventAttributes, ImageAnalysis
import os

@pytest.fixture
def integration():
    """Create an EventIntegration instance for testing"""
    return EventIntegration()

@pytest.fixture
def sample_event():
    """Create a sample event for testing"""
    start_time = datetime.utcnow()
    return Event(
        event_id="test-event-001",
        title="Test Music Festival",
        description="Join us for an amazing outdoor music festival! Live bands and great food.",
        short_description="Join us for an amazing outdoor music festival!",
        start_datetime=start_time,
        end_datetime=start_time + timedelta(hours=3),
        location=Location(
            venue_name="Test Venue",
            address="123 Test St",
            city="San Francisco",
            state="CA",
            country="United States",
            coordinates={"lat": 37.7749, "lng": -122.4194}
        ),
        categories=[],
        tags=[],
        price_info=PriceInfo(
            currency="USD",
            min_price=45.0,
            max_price=90.0,
            price_tier="medium"
        ),
        attributes=EventAttributes(),
        extracted_topics=[],
        sentiment_scores={"positive": 0.0, "negative": 0.0},
        images=["tests/data/test_image.jpg"],  # Add test image
        source=SourceInfo(
            platform="test",
            url="https://example.com/test-event",
            last_updated=datetime.utcnow()
        )
    )

@pytest.mark.asyncio
async def test_process_event_with_images(integration, sample_event):
    """Test processing a single event with image analysis"""
    # Process event
    processed_event = await integration.process_event(sample_event)
    
    # Verify event was processed
    assert processed_event.event_id == sample_event.event_id
    assert processed_event.title == sample_event.title
    
    # Verify image analysis was performed
    assert len(processed_event.image_analysis) == 1
    analysis = processed_event.image_analysis[0]
    
    # Verify scene classification
    assert analysis.scene_classification
    assert any(scene in ["concert_hall", "stage", "crowd"] for scene in analysis.scene_classification)
    assert any(conf > 0.5 for conf in analysis.scene_classification.values())
    
    # Verify crowd density
    assert analysis.crowd_density
    assert "density" in analysis.crowd_density
    assert "count_estimate" in analysis.crowd_density
    assert "confidence" in analysis.crowd_density
    
    # Verify objects
    assert analysis.objects
    assert any(obj["name"] in ["person", "crowd", "stage"] for obj in analysis.objects)
    assert any(obj["confidence"] > 0.5 for obj in analysis.objects)
    
    # Verify safe search
    assert analysis.safe_search
    assert all(k in analysis.safe_search for k in ["adult", "violence", "racy"])
    
    # Verify image analysis influenced categorization
    assert any(cat in processed_event.extracted_topics for cat in ["concert", "stage", "crowd"])
    assert processed_event.attributes.indoor_outdoor in ["indoor", "outdoor"]

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
    
    # Verify event structure and processing
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
        
        # Verify image processing if event has images
        if event.images:
            assert event.image_analysis
            for analysis in event.image_analysis:
                assert analysis.scene_classification
                assert analysis.crowd_density
                assert analysis.objects
                assert analysis.safe_search

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
    
    # Verify event structure and processing
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
        
        # Verify image processing if event has images
        if event.images:
            assert event.image_analysis
            for analysis in event.image_analysis:
                assert analysis.scene_classification
                assert analysis.crowd_density
                assert analysis.objects
                assert analysis.safe_search

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
    assert len(events) == 0
    
    # Test with invalid date range
    events = await integration.collect_and_process_events(
        location="San Francisco, CA",
        start_date=datetime.utcnow() + timedelta(days=7),
        end_date=datetime.utcnow()  # End date before start date
    )
    
    # Should return empty list on error, not raise exception
    assert isinstance(events, list)
    assert len(events) == 0 