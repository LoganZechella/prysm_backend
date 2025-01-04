import pytest
from datetime import datetime, timedelta, time
import copy
from unittest.mock import patch, MagicMock

from app.utils.schema import (
    Event, Location, PriceInfo, SourceInfo,
    ImageAnalysis, UserPreferences
)
from app.utils.event_processor import EventProcessor # type: ignore

@pytest.fixture
def event_processor():
    """Create an EventProcessor instance for testing"""
    return EventProcessor()

@pytest.fixture
def raw_events():
    """Create sample raw event data"""
    base_time = datetime.utcnow()
    return [
        {
            "event_id": "event1",
            "title": "Rock Concert",
            "description": "A great rock concert",
            "start_datetime": base_time.isoformat(),
            "location": {
                "venue_name": "Concert Hall",
                "address": "123 Music St",
                "city": "San Francisco",
                "state": "CA",
                "country": "United States",
                "coordinates": {"lat": 37.7749, "lng": -122.4194}
            },
            "categories": ["music", "concert", "rock"],
            "price_info": {
                "currency": "USD",
                "min_price": 50.0,
                "max_price": 100.0,
                "price_tier": "medium"
            },
            "source": {
                "platform": "test",
                "url": "https://example.com/event1",
                "last_updated": datetime.utcnow().isoformat()
            }
        },
        {
            "event_id": "event2",
            "title": "Jazz Night",
            "description": "Smooth jazz evening",
            "start_datetime": (base_time + timedelta(hours=2)).isoformat(),
            "location": {
                "venue_name": "Jazz Club",
                "address": "456 Jazz Ave",
                "city": "San Francisco",
                "state": "CA",
                "country": "United States",
                "coordinates": {"lat": 37.7848, "lng": -122.4294}
            },
            "categories": ["music", "jazz", "nightlife"],
            "price_info": {
                "currency": "USD",
                "min_price": 30.0,
                "max_price": 60.0,
                "price_tier": "medium"
            },
            "source": {
                "platform": "test",
                "url": "https://example.com/event2",
                "last_updated": datetime.utcnow().isoformat()
            }
        }
    ]

@pytest.fixture
def processed_events(raw_events):
    """Create sample processed events"""
    events = []
    for raw_event in raw_events:
        event = Event.from_raw_data(raw_event)
        event.image_analysis = [
            ImageAnalysis(
                url="https://example.com/image.jpg",
                scene_classification={
                    "concert_hall": 0.9,
                    "stage": 0.8,
                    "crowd": 0.7,
                    "indoor": 0.6
                },
                crowd_density={"density": "high", "count_estimate": "50+", "confidence": 0.9},
                objects=[{"name": "stage", "confidence": 0.95}],
                safe_search={"adult": "UNLIKELY", "violence": "UNLIKELY", "racy": "POSSIBLE"}
            )
        ]
        events.append(event)
    return events

@pytest.fixture
def user_preferences():
    """Create sample user preferences"""
    return UserPreferences(
        user_id="user123",
        preferred_categories=["music", "jazz", "nightlife"],
        price_preferences=["budget", "medium"],
        preferred_location={"lat": 37.7749, "lng": -122.4194},
        preferred_radius=5.0,
        preferred_times=[
            (time(18, 0), time(23, 0)),
            (time(12, 0), time(15, 0))
        ]
    )

@pytest.mark.asyncio
async def test_process_and_recommend_events(event_processor, raw_events, user_preferences):
    """Test processing events and generating recommendations"""
    with patch('app.utils.event_processing.enrich_event') as mock_enrich:
        # Mock the enrich_event function to return processed events
        async def mock_enrich_impl(event):
            event.image_analysis = [
                ImageAnalysis(
                    url="https://example.com/image.jpg",
                    scene_classification={
                        "concert_hall": 0.9,
                        "stage": 0.8,
                        "crowd": 0.7,
                        "indoor": 0.6
                    }
                )
            ]
            return event
        mock_enrich.side_effect = mock_enrich_impl
        
        # Test without user preferences
        events = await event_processor.process_and_recommend_events(raw_events)
        assert len(events) == 2
        assert events[0].event_id == "event1"
        assert events[1].event_id == "event2"
        
        # Test with user preferences
        events = await event_processor.process_and_recommend_events(
            raw_events,
            user_preferences,
            num_recommendations=1
        )
        assert len(events) == 1
        # Jazz event should be recommended due to category match
        assert events[0].event_id == "event2"

def test_get_similar_events(event_processor, processed_events):
    """Test getting similar events"""
    event = processed_events[0]
    similar = event_processor.get_similar_events(
        event,
        processed_events,
        num_similar=1
    )
    
    assert len(similar) == 1
    assert similar[0].event_id == "event2"  # Jazz event should be most similar to rock concert

def test_filter_events_by_preferences(event_processor, processed_events, user_preferences):
    """Test filtering events by user preferences"""
    # Test with high threshold to get only best matches
    filtered = event_processor.filter_events_by_preferences(
        processed_events,
        user_preferences,
        min_score=0.8
    )
    
    # With high threshold, only the jazz event should match
    assert len(filtered) == 1
    assert filtered[0].event_id == "event2"
    
    # Test with lower threshold to include more events
    filtered = event_processor.filter_events_by_preferences(
        processed_events,
        user_preferences,
        min_score=0.3
    )
    
    # Lower threshold should include both events
    assert len(filtered) == 2
    
    # Events should be in original order
    assert filtered[0].event_id == "event1"
    assert filtered[1].event_id == "event2" 