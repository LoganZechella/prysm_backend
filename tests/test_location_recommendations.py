import pytest
from datetime import datetime
from app.services.location_recommendations import (
    calculate_distance,
    filter_events_by_distance,
    sort_events_by_distance
)
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

def test_calculate_distance():
    """Test distance calculation between two points."""
    # San Francisco coordinates
    point1 = {"lat": 37.7749, "lon": -122.4194}
    # Los Angeles coordinates
    point2 = {"lat": 34.0522, "lon": -118.2437}
    
    distance = calculate_distance(point1, point2)
    assert distance > 0
    assert distance < 1000  # Distance should be less than 1000 km

def test_filter_events_by_distance():
    """Test filtering events by distance."""
    now = datetime.now()
    # Create test events
    events = [
        Event(
            event_id="1",
            title="SF Event",
            description="Event in San Francisco",
            start_datetime=now,
            location=Location(
                venue_name="SF Venue",
                city="San Francisco",
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
                last_updated=now
            ),
            metadata=EventMetadata(
                popularity_score=0.8
            )
        ),
        Event(
            event_id="2",
            title="LA Event",
            description="Event in Los Angeles",
            start_datetime=now,
            location=Location(
                venue_name="LA Venue",
                city="Los Angeles",
                coordinates={"lat": 34.0522, "lon": -118.2437}
            ),
            categories=["test"],
            attributes=EventAttributes(
                indoor_outdoor="indoor",
                age_restriction="all"
            ),
            source=SourceInfo(
                platform="test",
                url="http://test.com",
                last_updated=now
            ),
            metadata=EventMetadata(
                popularity_score=0.8
            )
        )
    ]
    
    # Filter from SF with 100km radius
    user_location = {"lat": 37.7749, "lon": -122.4194}
    max_distance = 100
    
    filtered = filter_events_by_distance(events, user_location, max_distance)
    assert len(filtered) == 1
    assert filtered[0].event_id == "1"

def test_sort_events_by_distance():
    """Test sorting events by distance."""
    now = datetime.now()
    # Create test events
    events = [
        Event(
            event_id="1",
            title="LA Event",
            description="Event in Los Angeles",
            start_datetime=now,
            location=Location(
                venue_name="LA Venue",
                city="Los Angeles",
                coordinates={"lat": 34.0522, "lon": -118.2437}
            ),
            categories=["test"],
            attributes=EventAttributes(
                indoor_outdoor="indoor",
                age_restriction="all"
            ),
            source=SourceInfo(
                platform="test",
                url="http://test.com",
                last_updated=now
            ),
            metadata=EventMetadata(
                popularity_score=0.8
            )
        ),
        Event(
            event_id="2",
            title="SF Event",
            description="Event in San Francisco",
            start_datetime=now,
            location=Location(
                venue_name="SF Venue",
                city="San Francisco",
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
                last_updated=now
            ),
            metadata=EventMetadata(
                popularity_score=0.8
            )
        )
    ]
    
    # Sort from SF
    user_location = {"lat": 37.7749, "lon": -122.4194}
    
    sorted_events = sort_events_by_distance(events, user_location)
    assert len(sorted_events) == 2
    assert sorted_events[0].event_id == "2"  # SF event should be first
    assert sorted_events[1].event_id == "1"  # LA event should be second 