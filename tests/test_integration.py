import pytest
from datetime import datetime, timedelta, time
from typing import List, Dict, Any
from app.utils.recommendation import RecommendationEngine
from app.utils.event_processor import EventProcessor
from app.utils.schema import Event, UserPreferences, Location, PriceInfo, SourceInfo, ImageAnalysis, EventAttributes
from app.utils.category_hierarchy import create_default_hierarchy

@pytest.fixture
def sample_user_preferences():
    """Create sample user preferences"""
    return UserPreferences(
        user_id="test_user_123",
        preferred_categories=["technology", "education", "networking"],
        price_preferences=["free", "budget"],
        preferred_location={
            "lat": 37.7749,
            "lng": -122.4194
        },
        preferred_radius=5.0,
        preferred_times=[
            (time(18, 0), time(23, 0)),  # Evening: 6 PM to 11 PM
            (time(9, 0), time(17, 0))    # Day: 9 AM to 5 PM
        ]
    )

@pytest.fixture
def sample_events():
    """Create sample events across different categories and locations"""
    # Use a fixed base time at noon
    base_time = datetime(2024, 1, 1, 12, 0)  # 12 PM on Jan 1, 2024
    events = []
    hierarchy = create_default_hierarchy()
    
    # Tech Meetup - Evening, Free, Close location
    event = Event(
        event_id="tech1",
        title="Python Developers Meetup",
        description="Monthly meetup for Python developers",
        start_datetime=datetime.combine(base_time.date(), time(19, 0)),  # 7 PM
        location=Location(
            venue_name="Tech Hub",
            address="123 Tech St",
            city="San Francisco",
            state="CA",
            country="US",
            coordinates={"lat": 37.7749, "lng": -122.4194}  # Same as user's location
        ),
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
        ]
    )
    event.add_category("technology", hierarchy)
    event.add_category("education", hierarchy)
    event.add_category("networking", hierarchy)
    events.append(event)
    
    # Another Tech Event - Evening, Budget price, Close location
    event = Event(
        event_id="tech2",
        title="Web Development Workshop",
        description="Learn modern web development",
        start_datetime=datetime.combine(base_time.date(), time(20, 0)),  # 8 PM
        location=Location(
            venue_name="Code Academy",
            address="456 Dev St",
            city="San Francisco",
            state="CA",
            country="US",
            coordinates={"lat": 37.7740, "lng": -122.4180}  # Close to user's location
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=10.0,
            max_price=10.0,
            price_tier="budget"
        ),
        source=SourceInfo(
            platform="eventbrite",
            url="https://example.com/tech2",
            last_updated=datetime.now()
        ),
        image_analysis=[
            ImageAnalysis(
                url="https://example.com/tech2.jpg",
                scene_classification={
                    "indoor": 0.9,
                    "crowd": 0.6
                }
            )
        ]
    )
    event.add_category("technology", hierarchy)
    event.add_category("education", hierarchy)
    events.append(event)
    
    # Art Exhibition - Daytime, Budget price, Close location
    event = Event(
        event_id="art1",
        title="Modern Art Exhibition",
        description="Contemporary art showcase",
        start_datetime=datetime.combine(base_time.date(), time(14, 0)),  # 2 PM
        location=Location(
            venue_name="Art Gallery",
            address="789 Art St",
            city="San Francisco",
            state="CA",
            country="US",
            coordinates={"lat": 37.7700, "lng": -122.4150}  # ~0.7km from user
        ),
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
        ]
    )
    event.add_category("art", hierarchy)
    event.add_category("exhibition", hierarchy)
    event.add_category("culture", hierarchy)
    events.append(event)
    
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
    print(f"\nFiltered events: {len(filtered_events)}")
    for event in filtered_events:
        print(f"- {event.title}: {event.start_datetime.time()}")
        print(f"  Categories: {event.categories}")
    
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
    
    # Filter out recommendations with no matching categories
    recommendations = [(event, score) for event, score in recommendations
                      if any(cat in sample_user_preferences.preferred_categories 
                            for cat in event.categories)]
    
    # Verify recommendations
    assert len(recommendations) > 0
    print(f"\nRecommendations: {len(recommendations)}")
    for event, score in recommendations:
        print(f"- {event.title}: score={score:.2f}, time={event.start_datetime.time()}")
        print(f"  Categories: {event.categories}")
        
        # Verify score is between 0 and 1
        assert 0 <= score <= 1
        
        # Verify event matches user preferences
        assert any(cat in sample_user_preferences.preferred_categories 
                  for cat in event.categories)
        assert event.price_info.price_tier in sample_user_preferences.price_preferences
        
        # Verify time preferences
        event_time = event.start_datetime.time()
        time_match = False
        print(f"\nChecking time slots for {event.title}:")
        print(f"Event time: {event_time}")
        for start, end in sample_user_preferences.preferred_times:
            print(f"Slot: {start} - {end}")
            if start <= event_time <= end:
                time_match = True
                print("Match found!")
                break
        assert time_match, f"Event {event.title} at {event_time} doesn't match any preferred time slots"
    
    # 3. Get similar events for top recommendation
    top_event = recommendations[0][0]
    similar_events = event_processor.recommendation_engine.get_similar_events(
        top_event,
        [event for event in sample_events if event.event_id != top_event.event_id]  # Exclude top event from candidates
    )
    
    # Verify similar events
    assert len(similar_events) > 0, "Should find at least one similar event"
    
    # Print similar events for debugging
    print("\nSimilar events:")
    for event, score in similar_events:
        print(f"- {event.title}: score={score:.2f}")
        print(f"  Categories: {event.categories}")
    
    # Get first similar event
    similar_event, similarity_score = similar_events[0]
    assert similar_event.event_id != top_event.event_id  # Should not include the input event
    assert 0 <= similarity_score <= 1  # Similarity score should be between 0 and 1
    
    # Similar events should share some categories with top event
    for event, score in similar_events:
        common_categories = set(event.categories) & set(top_event.categories)
        assert len(common_categories) > 0, f"Event {event.title} shares no categories with {top_event.title}"
        assert 0 <= score <= 1  # Similarity score should be between 0 and 1
    
    # 4. Verify recommendation ordering
    # First recommendation should have highest score
    scores = [score for _, score in recommendations]
    assert all(scores[i] >= scores[i+1] for i in range(len(scores)-1))
    
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