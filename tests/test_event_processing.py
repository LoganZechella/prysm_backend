import pytest
from datetime import datetime
from app.services.event_processing import (
    clean_event_data,
    extract_topics,
    calculate_event_scores
)
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata
from app.schemas.category import CategoryHierarchy

def test_clean_event_data():
    """Test cleaning event data."""
    event = Event(
        event_id="test-1",
        title="  Test Event  ",
        description="A test event\n\nWith multiple lines",
        start_datetime=datetime(2024, 1, 1),
        location=Location(
            venue_name="Test Venue",
            city="Test City",
            state="  TS  ",
            country="  Test Country  ",
            coordinates={"lat": 37.7749, "lon": -122.4194}
        ),
        categories=["test"],
        attributes=EventAttributes(
            indoor_outdoor="indoor",
            age_restriction="all"
        ),
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=datetime(2024, 1, 1)
        ),
        metadata=EventMetadata(
            popularity_score=0.8
        )
    )
    
    cleaned = clean_event_data(event)
    assert cleaned.title == "Test Event"
    assert cleaned.description == "A test event With multiple lines"
    assert cleaned.location.state == "TS"
    assert cleaned.location.country == "Test Country"

def test_extract_topics():
    """Test topic extraction from event text."""
    text = "Join us for a live music concert featuring jazz and blues bands"
    topics = extract_topics(text)
    assert "music" in topics
    assert "jazz" in topics
    assert "blues" in topics

def test_calculate_event_scores():
    """Test calculating event scores."""
    event = Event(
        event_id="test-1",
        title="Jazz Concert",
        description="Live jazz music performance",
        start_datetime=datetime(2024, 1, 1),
        location=Location(
            venue_name="Jazz Club",
            city="Test City",
            coordinates={"lat": 37.7749, "lon": -122.4194}
        ),
        categories=["music", "jazz"],
        attributes=EventAttributes(
            indoor_outdoor="indoor",
            age_restriction="all"
        ),
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=datetime(2024, 1, 1)
        ),
        metadata=EventMetadata(
            popularity_score=0.8
        ),
        price_info=PriceInfo(
            min_price=20.0,
            max_price=50.0,
            currency="USD",
            price_tier="medium"
        )
    )
    
    user_preferences = {
        "preferred_categories": ["music", "jazz"],
        "excluded_categories": ["sports"],
        "min_price": 0.0,
        "max_price": 100.0,
        "preferred_location": {"lat": 37.7749, "lon": -122.4194},
        "max_distance": 10.0
    }
    
    scores = calculate_event_scores(event, user_preferences)
    assert "category_score" in scores
    assert "price_score" in scores
    assert "distance_score" in scores
    assert "popularity_score" in scores
    assert "total_score" in scores
    
    assert scores["category_score"] > 0.8  # High category match
    assert scores["price_score"] > 0.7  # Price within range
    assert scores["distance_score"] > 0.9  # Very close to preferred location 