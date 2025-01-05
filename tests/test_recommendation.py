import pytest
from datetime import datetime, timedelta
from app.utils.schema import Event, UserPreferences, Location, PriceInfo, SourceInfo, EventMetadata
from app.utils.recommendation import RecommendationEngine

@pytest.fixture
def recommendation_engine():
    return RecommendationEngine(cache_size=100, cache_ttl=60)

@pytest.fixture
def sample_events():
    base_time = datetime.now()
    events = []
    
    # Event 1: Music concert
    events.append(Event(
        event_id="event1",
        title="Rock Concert",
        description="Amazing rock concert",
        start_datetime=base_time + timedelta(days=1),
        location=Location(
            venue_name="Concert Hall",
            city="San Francisco",
            coordinates={"lat": 37.7749, "lng": -122.4194}
        ),
        categories=["music", "concert", "rock"],
        price_info=PriceInfo(min_price=50.0, max_price=150.0, price_tier="medium"),
        source=SourceInfo(
            platform="ticketmaster",
            url="http://example.com/1",
            last_updated=base_time
        ),
        metadata=EventMetadata(popularity_score=0.8),
        category_hierarchy={
            "music": ["entertainment"],
            "concert": ["music", "entertainment"],
            "rock": ["music", "entertainment"]
        }
    ))
    
    # Event 2: Art exhibition
    events.append(Event(
        event_id="event2",
        title="Modern Art Exhibition",
        description="Contemporary art showcase",
        start_datetime=base_time + timedelta(days=2),
        location=Location(
            venue_name="Art Gallery",
            city="San Francisco",
            coordinates={"lat": 37.7746, "lng": -122.4190}
        ),
        categories=["art", "exhibition", "culture"],
        price_info=PriceInfo(min_price=20.0, max_price=30.0, price_tier="budget"),
        source=SourceInfo(
            platform="eventbrite",
            url="http://example.com/2",
            last_updated=base_time
        ),
        metadata=EventMetadata(popularity_score=0.7),
        category_hierarchy={
            "art": ["culture"],
            "exhibition": ["art", "culture"],
            "culture": []
        }
    ))
    
    # Event 3: Tech conference
    events.append(Event(
        event_id="event3",
        title="Tech Conference 2024",
        description="Latest in technology",
        start_datetime=base_time + timedelta(days=3),
        location=Location(
            venue_name="Convention Center",
            city="San Francisco",
            coordinates={"lat": 37.7833, "lng": -122.4167}
        ),
        categories=["technology", "conference", "business"],
        price_info=PriceInfo(min_price=200.0, max_price=500.0, price_tier="premium"),
        source=SourceInfo(
            platform="eventbrite",
            url="http://example.com/3",
            last_updated=base_time
        ),
        metadata=EventMetadata(popularity_score=0.9),
        category_hierarchy={
            "technology": ["business"],
            "conference": ["business"],
            "business": []
        }
    ))
    
    return events

@pytest.fixture
def sample_user_preferences():
    return UserPreferences(
        user_id="user1",
        preferred_categories=["music", "art"],
        min_price=0.0,
        max_price=200.0,
        preferred_location={"lat": 37.7749, "lng": -122.4194},
        max_distance=10.0,
        preferred_days=["monday", "friday", "saturday"],
        min_rating=0.6
    )

def test_event_similarity_calculation(recommendation_engine, sample_events):
    """Test event similarity calculation"""
    event1, event2, event3 = sample_events
    
    # Similar events (both are entertainment)
    similarity1_2 = recommendation_engine.calculate_event_similarity(event1, event2)
    assert 0 <= similarity1_2 <= 1
    
    # Less similar events (music vs tech)
    similarity1_3 = recommendation_engine.calculate_event_similarity(event1, event3)
    assert 0 <= similarity1_3 <= 1
    
    # Similarity should be symmetric
    similarity2_1 = recommendation_engine.calculate_event_similarity(event2, event1)
    assert abs(similarity1_2 - similarity2_1) < 1e-6
    
    # Event should be perfectly similar to itself
    self_similarity = recommendation_engine.calculate_event_similarity(event1, event1)
    assert self_similarity > 0.99

def test_similarity_caching(recommendation_engine, sample_events):
    """Test that similarity calculations are cached"""
    event1, event2, _ = sample_events
    
    # First calculation should cache the result
    similarity1 = recommendation_engine.calculate_event_similarity(event1, event2)
    
    # Second calculation should use cached result
    similarity2 = recommendation_engine.calculate_event_similarity(event1, event2)
    
    assert similarity1 == similarity2
    assert f"{event1.event_id}:{event2.event_id}" in recommendation_engine._similarity_cache

def test_personalized_recommendations(recommendation_engine, sample_events, sample_user_preferences):
    """Test personalized event recommendations"""
    recommendations = recommendation_engine.get_personalized_recommendations(
        sample_user_preferences,
        sample_events,
        n=2
    )
    
    assert len(recommendations) <= 2
    assert all(isinstance(r, tuple) and len(r) == 2 for r in recommendations)
    assert all(0 <= score <= 1 for _, score in recommendations)
    
    # First recommendation should be music event (matches user preferences better)
    first_event, _ = recommendations[0]
    assert "music" in first_event.categories

def test_preference_score_calculation(recommendation_engine, sample_events, sample_user_preferences):
    """Test individual preference score calculations"""
    event = sample_events[0]  # Music concert
    
    score = recommendation_engine._calculate_preference_score(
        event,
        set(sample_user_preferences.preferred_categories),
        (sample_user_preferences.min_price, sample_user_preferences.max_price),
        sample_user_preferences.preferred_location,
        sample_user_preferences
    )
    
    assert 0 <= score <= 1
    
    # Test with missing location
    no_location_prefs = sample_user_preferences.copy()
    no_location_prefs.preferred_location = None
    
    score_no_location = recommendation_engine._calculate_preference_score(
        event,
        set(no_location_prefs.preferred_categories),
        (no_location_prefs.min_price, no_location_prefs.max_price),
        no_location_prefs.preferred_location,
        no_location_prefs
    )
    
    assert 0 <= score_no_location <= 1

def test_excluded_categories(recommendation_engine, sample_events, sample_user_preferences):
    """Test that excluded categories are respected"""
    # Add technology to excluded categories
    sample_user_preferences.excluded_categories = ["technology"]
    
    recommendations = recommendation_engine.get_personalized_recommendations(
        sample_user_preferences,
        sample_events
    )
    
    # Verify tech conference is not in recommendations
    recommended_categories = set()
    for event, _ in recommendations:
        recommended_categories.update(event.categories)
    
    assert "technology" not in recommended_categories

def test_minimum_rating(recommendation_engine, sample_events, sample_user_preferences):
    """Test that minimum rating threshold is respected"""
    # Set high minimum rating
    sample_user_preferences.min_rating = 0.85
    
    recommendations = recommendation_engine.get_personalized_recommendations(
        sample_user_preferences,
        sample_events
    )
    
    # Only tech conference should meet the rating threshold
    assert len(recommendations) == 1
    event, _ = recommendations[0]
    assert event.event_id == "event3" 