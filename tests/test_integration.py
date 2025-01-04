import pytest
from datetime import datetime, timedelta, time
from typing import List, Dict, Any
from app.utils.recommendation import RecommendationEngine
from app.utils.event_processor import EventProcessor
from app.utils.schema import Event, UserPreferences, Location, PriceInfo, SourceInfo, ImageAnalysis, EventAttributes

@pytest.fixture
def sample_user_preferences():
    """Create sample user preferences"""
    return UserPreferences(
        user_id="test_user_123",
        preferred_categories=["music", "technology", "art"],
        price_preferences=["free", "budget", "medium"],
        preferred_location={
            "lat": 37.7749,
            "lng": -122.4194  # San Francisco coordinates
        },
        preferred_radius=10.0,  # 10km radius
        preferred_times=[
            (time(18, 0), time(23, 0)),  # 6 PM to 11 PM
            (time(9, 0), time(17, 0))    # 9 AM to 5 PM
        ]
    )

@pytest.fixture
def sample_events():
    """Create sample events across different categories and locations"""
    base_time = datetime.now()
    events = []
    
    # Tech Meetup - Evening, Free, Close location
    events.append(Event(
        event_id="tech1",
        title="Python Developers Meetup",
        description="Monthly meetup for Python developers",
        start_datetime=base_time + timedelta(days=1, hours=19),  # 7 PM
        location=Location(
            venue_name="Tech Hub",
            address="123 Tech St",
            city="San Francisco",
            state="CA",
            country="US",
            coordinates={"lat": 37.7749, "lng": -122.4194}
        ),
        categories=["technology", "networking", "education"],
        price_info=PriceInfo(
            currency="USD",
            min_price=0.0,
            max_price=0.0,
            price_tier="free"
        ),
        source=SourceInfo(
            platform="meetup",
            url="https://example.com/tech1",
            last_updated=datetime.now()
        ),
        image_analysis=[
            ImageAnalysis(
                url="https://example.com/tech1.jpg",
                scene_classification={
                    "indoor": 0.9,
                    "crowd": 0.7
                }
            )
        ],
        attributes=EventAttributes()
    ))
    
    # Concert - Evening, Medium price, Close location
    events.append(Event(
        event_id="music1",
        title="Jazz Night",
        description="Live jazz performance",
        start_datetime=base_time + timedelta(days=2, hours=20),  # 8 PM
        location=Location(
            venue_name="Music Hall",
            address="456 Music Ave",
            city="San Francisco",
            state="CA",
            country="US",
            coordinates={"lat": 37.7848, "lng": -122.4294}
        ),
        categories=["music", "jazz", "live_entertainment"],
        price_info=PriceInfo(
            currency="USD",
            min_price=40.0,
            max_price=80.0,
            price_tier="medium"
        ),
        source=SourceInfo(
            platform="ticketmaster",
            url="https://example.com/music1",
            last_updated=datetime.now()
        ),
        image_analysis=[
            ImageAnalysis(
                url="https://example.com/music1.jpg",
                scene_classification={
                    "concert_hall": 0.9,
                    "stage": 0.8,
                    "crowd": 0.7,
                    "indoor": 0.9
                }
            )
        ],
        attributes=EventAttributes()
    ))
    
    # Art Exhibition - Daytime, Budget price, Close location
    events.append(Event(
        event_id="art1",
        title="Modern Art Exhibition",
        description="Contemporary art showcase",
        start_datetime=base_time + timedelta(days=1, hours=14),  # 2 PM
        location=Location(
            venue_name="Art Gallery",
            address="789 Art St",
            city="San Francisco",
            state="CA",
            country="US",
            coordinates={"lat": 37.7694, "lng": -122.4862}
        ),
        categories=["art", "exhibition", "culture"],
        price_info=PriceInfo(
            currency="USD",
            min_price=15.0,
            max_price=15.0,
            price_tier="budget"
        ),
        source=SourceInfo(
            platform="eventbrite",
            url="https://example.com/art1",
            last_updated=datetime.now()
        ),
        image_analysis=[
            ImageAnalysis(
                url="https://example.com/art1.jpg",
                scene_classification={
                    "art_gallery": 0.9,
                    "indoor": 0.8
                }
            )
        ],
        attributes=EventAttributes()
    ))
    
    # Premium Concert - Evening, Premium price, Far location
    events.append(Event(
        event_id="music2",
        title="Rock Concert",
        description="Major rock band performance",
        start_datetime=base_time + timedelta(days=3, hours=19),  # 7 PM
        location=Location(
            venue_name="Stadium",
            address="1000 Stadium Way",
            city="Oakland",
            state="CA",
            country="US",
            coordinates={"lat": 37.8044, "lng": -122.2711}
        ),
        categories=["music", "rock", "concert"],
        price_info=PriceInfo(
            currency="USD",
            min_price=100.0,
            max_price=300.0,
            price_tier="premium"
        ),
        source=SourceInfo(
            platform="ticketmaster",
            url="https://example.com/music2",
            last_updated=datetime.now()
        ),
        image_analysis=[
            ImageAnalysis(
                url="https://example.com/music2.jpg",
                scene_classification={
                    "stadium": 0.9,
                    "stage": 0.8,
                    "crowd": 0.9,
                    "outdoor": 0.9
                }
            )
        ],
        attributes=EventAttributes()
    ))
    
    return events

def test_end_to_end_recommendation_flow(sample_user_preferences, sample_events):
    """Test the complete flow from user preferences to event recommendations"""
    
    # Initialize components
    event_processor = EventProcessor()
    
    # 1. Filter events by user preferences
    filtered_events = event_processor.filter_events_by_preferences(
        sample_events,
        sample_user_preferences
    )
    
    # Verify basic filtering
    assert len(filtered_events) > 0
    assert len(filtered_events) < len(sample_events)  # Some events should be filtered out
    
    # Premium price events should be filtered out
    assert not any(event.price_info.price_tier == "premium" for event in filtered_events)
    
    # Events should be within preferred radius
    for event in filtered_events:
        distance = event_processor.recommendation_engine._haversine_distance(
            event.location.coordinates['lat'],
            event.location.coordinates['lng'],
            sample_user_preferences.preferred_location['lat'],
            sample_user_preferences.preferred_location['lng']
        )
        assert distance <= sample_user_preferences.preferred_radius
    
    # 2. Get personalized recommendations
    recommendations = event_processor.recommendation_engine.get_personalized_recommendations(
        sample_user_preferences,
        filtered_events
    )
    
    # Verify recommendations
    assert len(recommendations) > 0
    for event, score in recommendations:
        # Verify score is between 0 and 1
        assert 0 <= score <= 1
        
        # Verify event matches user preferences
        assert any(cat in sample_user_preferences.preferred_categories 
                  for cat in event.categories)
        assert event.price_info.price_tier in sample_user_preferences.price_preferences
        
        # Verify time preferences
        event_time = event.start_datetime.time()
        time_match = False
        for start, end in sample_user_preferences.preferred_times:
            if start <= event_time <= end:
                time_match = True
                break
        assert time_match
    
    # 3. Get similar events for top recommendation
    top_event = recommendations[0][0]
    similar_events = event_processor.get_similar_events(
        top_event,
        filtered_events
    )
    
    # Verify similar events
    assert len(similar_events) > 0
    assert similar_events[0] != top_event  # Should not include the input event
    
    # Similar events should share some categories with top event
    for event in similar_events:
        common_categories = set(event.categories) & set(top_event.categories)
        assert len(common_categories) > 0
    
    # 4. Verify recommendation ordering
    # First recommendation should have highest score
    scores = [score for _, score in recommendations]
    assert scores == sorted(scores, reverse=True)
    
    # Events in preferred categories should be ranked higher
    for i in range(len(recommendations)-1):
        event1, score1 = recommendations[i]
        event2, score2 = recommendations[i+1]
        if score1 > score2:
            # Higher scored event should have more matching categories
            matches1 = len(set(event1.categories) & 
                         set(sample_user_preferences.preferred_categories))
            matches2 = len(set(event2.categories) & 
                         set(sample_user_preferences.preferred_categories))
            assert matches1 >= matches2

def test_recommendation_edge_cases(sample_user_preferences, sample_events):
    """Test recommendation system with edge cases"""
    
    event_processor = EventProcessor()
    
    # Test with empty events list
    recommendations = event_processor.recommendation_engine.get_personalized_recommendations(
        sample_user_preferences,
        []
    )
    assert len(recommendations) == 0
    
    # Test with no matching events
    # Modify preferences to exclude all events
    no_match_preferences = UserPreferences(
        user_id="test_user_456",
        preferred_categories=["sports"],  # No sports events in sample
        price_preferences=["free"],
        preferred_location={
            "lat": 0.0,  # Far from all events
            "lng": 0.0
        },
        preferred_radius=1.0,
        preferred_times=[(time(3, 0), time(4, 0))]  # No events at this time
    )
    
    filtered_events = event_processor.filter_events_by_preferences(
        sample_events,
        no_match_preferences
    )
    assert len(filtered_events) == 0
    
    # Test with single event
    single_event = [sample_events[0]]
    recommendations = event_processor.recommendation_engine.get_personalized_recommendations(
        sample_user_preferences,
        single_event
    )
    assert len(recommendations) == 1
    
    # Test similar events with single event
    similar_events = event_processor.get_similar_events(
        sample_events[0],
        single_event
    )
    assert len(similar_events) == 0  # No similar events when only one event exists 