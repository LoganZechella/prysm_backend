import pytest
from datetime import datetime, timedelta
from app.utils.event_processing import (
    clean_event_data,
    extract_topics,
    calculate_sentiment_scores,
    enrich_event,
    classify_event,
    process_event
)
from app.utils.schema import Event, Location, PriceInfo, SourceInfo, EventAttributes
from typing import cast

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
        images=[],
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

def test_enrich_event(sample_event):
    """Test event enrichment"""
    enriched_event = enrich_event(sample_event)
    
    assert "music" in enriched_event.extracted_topics
    assert enriched_event.attributes.indoor_outdoor == "outdoor"
    assert enriched_event.attributes.age_restriction == "21+"
    assert enriched_event.sentiment_scores["positive"] > 0

def test_classify_event(sample_event):
    """Test event classification"""
    classified_event = classify_event(sample_event)
    
    assert classified_event.price_info.price_tier == "medium"
    assert len(classified_event.categories) > 0
    assert classified_event.attributes.dress_code == "casual"

def test_process_event(sample_event):
    """Test complete event processing pipeline"""
    processed_event = process_event(sample_event)
    
    # Verify cleaning
    assert processed_event.title == "Test Music Festival"
    assert processed_event.location.city == "San Francisco"
    
    # Verify enrichment
    assert "music" in processed_event.extracted_topics
    assert processed_event.attributes.age_restriction == "21+"
    
    # Verify classification
    assert processed_event.price_info.price_tier == "medium"
    assert len(processed_event.categories) > 0

def test_process_event_error_handling():
    """Test error handling in event processing"""
    with pytest.raises(ValueError):
        process_event(cast(Event, None))  # Type cast None to Event to satisfy type checker 