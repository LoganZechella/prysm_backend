import pytest
from datetime import datetime, timedelta
from app.utils.event_processing import (
    clean_event_data,
    extract_topics,
    calculate_sentiment_scores,
    enrich_event,
    classify_event,
    process_event,
    process_images
)
from app.utils.schema import Event, Location, PriceInfo, SourceInfo, EventAttributes, ImageAnalysis
from typing import cast
import os

@pytest.fixture
def sample_event():
    """Create a sample event for testing"""
    start_time = datetime.utcnow()
    return Event(
        event_id="test-event-001",
        title=" Test Music Festival  ",  # Extra spaces to test cleaning
        description="  Join us for an amazing outdoor music festival! Live bands and great food. 21+ only.  ",
        short_description="",  # Empty string instead of None
        start_datetime=start_time,
        end_datetime=start_time + timedelta(hours=3),  # 3 hours after start
        location=Location(
            venue_name="Test Venue",
            address="123 test st",
            city=" san francisco ",
            state=" ca ",
            country=" united states ",
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

def test_clean_event_data(sample_event):
    """Test event data cleaning"""
    cleaned_event = clean_event_data(sample_event)
    
    assert cleaned_event.title == "Test Music Festival"
    assert cleaned_event.description == "Join us for an amazing outdoor music festival! Live bands and great food. 21+ only."
    assert cleaned_event.short_description  # Verify it's not empty
    assert len(cleaned_event.short_description) <= 203  # 200 chars + "..."
    assert cleaned_event.location.city == "San Francisco"
    assert cleaned_event.location.state == "CA"
    assert cleaned_event.location.country == "United States"
    assert cleaned_event.end_datetime is not None

def test_extract_topics():
    """Test topic extraction"""
    text = "Join us for a music festival with live bands! Tech workshops and business networking."
    topics = extract_topics(text)
    
    assert "music" in topics
    assert "technology" in topics
    assert "business" in topics
    assert "art" not in topics

def test_calculate_sentiment_scores():
    """Test sentiment score calculation"""
    # Test positive sentiment
    positive_text = "This is an amazing and exciting event! It will be fantastic and fun."
    positive_scores = calculate_sentiment_scores(positive_text)
    assert positive_scores["positive"] > positive_scores["negative"]
    assert positive_scores["positive"] + positive_scores["negative"] == 1.0
    
    # Test negative sentiment
    negative_text = "Unfortunately this event has been cancelled. Such bad news."
    negative_scores = calculate_sentiment_scores(negative_text)
    assert negative_scores["negative"] > negative_scores["positive"]
    assert negative_scores["positive"] + negative_scores["negative"] == 1.0
    
    # Test neutral sentiment
    neutral_text = "This is an event happening on Monday."
    neutral_scores = calculate_sentiment_scores(neutral_text)
    assert neutral_scores["positive"] == neutral_scores["negative"] == 0.5
    assert neutral_scores["positive"] + neutral_scores["negative"] == 1.0

@pytest.mark.asyncio
async def test_process_images(sample_event):
    """Test image processing"""
    # Process images
    processed_event = await process_images(sample_event)
    
    # Verify image analysis results
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

@pytest.mark.asyncio
async def test_enrich_event_with_images(sample_event):
    """Test event enrichment with image analysis"""
    # Enrich event
    enriched_event = await enrich_event(sample_event)
    
    # Verify image analysis was performed
    assert len(enriched_event.image_analysis) == 1
    
    # Verify image analysis influenced categorization
    assert any(cat in enriched_event.extracted_topics for cat in ["concert", "stage", "crowd"])
    
    # Verify indoor/outdoor classification was influenced by image analysis
    assert enriched_event.attributes.indoor_outdoor in ["indoor", "outdoor"]
    
    # Verify sentiment scores
    assert enriched_event.sentiment_scores["positive"] > 0
    assert enriched_event.sentiment_scores["negative"] >= 0
    assert enriched_event.sentiment_scores["positive"] + enriched_event.sentiment_scores["negative"] == 1.0

@pytest.mark.asyncio
async def test_process_event(sample_event):
    """Test complete event processing pipeline"""
    # Process event
    processed_event = await process_event(sample_event)
    
    # Verify cleaning
    assert processed_event.title == "Test Music Festival"
    assert processed_event.location.city == "San Francisco"
    
    # Verify enrichment
    assert "music" in processed_event.extracted_topics
    assert processed_event.attributes.age_restriction == "21+"
    
    # Verify classification
    assert processed_event.price_info.price_tier == "medium"
    assert len(processed_event.categories) > 0

@pytest.mark.asyncio
async def test_error_handling():
    """Test error handling in event processing"""
    with pytest.raises(ValueError):
        await process_event(cast(Event, None))  # Type cast None to Event to satisfy type checker 