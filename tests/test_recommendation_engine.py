import pytest
from datetime import datetime, timedelta
from app.recommendation.engine import RecommendationEngine
from app.models.event import Event
from app.models.preferences import UserPreferences

@pytest.fixture
def recommendation_engine():
    return RecommendationEngine()

@pytest.fixture
def sample_events():
    now = datetime.utcnow()
    return [
        Event(
            id=1,
            title="Rock Concert in the Park",
            description="Live rock music festival featuring local bands",
            categories=["music", "entertainment", "outdoor"],
            start_time=now + timedelta(days=3),
            location={"lat": 40.7128, "lng": -74.0060},  # NYC
            price_info={
                "type": "fixed",
                "amount": 50.0,
                "currency": "USD"
            },
            view_count=500
        ),
        Event(
            id=2,
            title="Tech Conference 2024",
            description="Annual technology conference with workshops",
            categories=["technology", "education", "networking"],
            start_time=now + timedelta(days=10),
            location={"lat": 37.7749, "lng": -122.4194},  # SF
            price_info={
                "type": "range",
                "min_amount": 200.0,
                "max_amount": 500.0,
                "currency": "USD"
            },
            view_count=1000
        ),
        Event(
            id=3,
            title="Free Yoga in the Park",
            description="Morning yoga session for all levels",
            categories=["wellness", "outdoor"],
            start_time=now + timedelta(days=1),
            location={"lat": 40.7829, "lng": -73.9654},  # Central Park
            price_info={
                "type": "fixed",
                "amount": 0.0,
                "currency": "USD"
            },
            view_count=100
        )
    ]

@pytest.fixture
def sample_preferences():
    return UserPreferences(
        user_id="test_user",
        preferred_categories=["music", "outdoor"],
        excluded_categories=["technology"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={
            "lat": 40.7128,
            "lng": -74.0060,
            "max_distance_km": 50
        },
        preferred_days=["Monday", "Friday", "Saturday"],
        preferred_times=["morning", "evening"]
    )

def test_score_event(recommendation_engine, sample_events, sample_preferences):
    """Test event scoring based on user preferences."""
    # Test scoring for rock concert (should be high score)
    concert_score = recommendation_engine.score_event(sample_events[0], sample_preferences)
    assert 0.0 <= concert_score <= 1.0
    assert concert_score > 0.7  # High score expected due to matching preferences
    
    # Test scoring for tech conference (should be low score)
    conference_score = recommendation_engine.score_event(sample_events[1], sample_preferences)
    assert 0.0 <= conference_score <= 1.0
    assert conference_score < 0.3  # Low score expected due to excluded category
    
    # Test scoring for yoga (should be medium score)
    yoga_score = recommendation_engine.score_event(sample_events[2], sample_preferences)
    assert 0.0 <= yoga_score <= 1.0
    assert 0.3 <= yoga_score <= 0.8  # Medium score expected

def test_category_score(recommendation_engine, sample_events, sample_preferences):
    """Test category matching score calculation."""
    # Test exact category match
    score = recommendation_engine._calculate_category_score(sample_events[0], sample_preferences)
    assert score > 0.5  # Should be high due to matching categories
    
    # Test excluded category
    score = recommendation_engine._calculate_category_score(sample_events[1], sample_preferences)
    assert score == 0.0  # Should be zero due to excluded category
    
    # Test partial category match
    score = recommendation_engine._calculate_category_score(sample_events[2], sample_preferences)
    assert 0.0 < score < 1.0  # Should be medium due to partial match

def test_location_score(recommendation_engine, sample_events, sample_preferences):
    """Test location-based score calculation."""
    # Test nearby event
    score = recommendation_engine._calculate_location_score(sample_events[0], sample_preferences)
    assert score > 0.8  # Should be high due to close proximity
    
    # Test distant event
    score = recommendation_engine._calculate_location_score(sample_events[1], sample_preferences)
    assert score == 0.0  # Should be zero due to being outside max distance
    
    # Test moderately distant event
    score = recommendation_engine._calculate_location_score(sample_events[2], sample_preferences)
    assert 0.0 < score < 1.0  # Should be medium due to moderate distance

def test_price_score(recommendation_engine, sample_events, sample_preferences):
    """Test price-based score calculation."""
    # Test within price range
    score = recommendation_engine._calculate_price_score(sample_events[0], sample_preferences)
    assert 0.0 < score < 1.0  # Should be medium due to being within range
    
    # Test above max price
    score = recommendation_engine._calculate_price_score(sample_events[1], sample_preferences)
    assert score == 0.0  # Should be zero due to being above max price
    
    # Test free event
    score = recommendation_engine._calculate_price_score(sample_events[2], sample_preferences)
    assert score == 1.0  # Should be maximum due to being free

def test_time_score(recommendation_engine, sample_events, sample_preferences):
    """Test time-based relevance score calculation."""
    # Test very soon event
    score = recommendation_engine._calculate_time_score(sample_events[2], sample_preferences)
    assert score > 0.8  # Should be high due to being very soon
    
    # Test moderately soon event
    score = recommendation_engine._calculate_time_score(sample_events[0], sample_preferences)
    assert 0.5 < score < 1.0  # Should be medium-high
    
    # Test distant event
    score = recommendation_engine._calculate_time_score(sample_events[1], sample_preferences)
    assert 0.3 < score < 0.85  # Should be medium due to being further in future

def test_popularity_score(recommendation_engine, sample_events):
    """Test popularity-based score calculation."""
    # Test high view count
    score = recommendation_engine._calculate_popularity_score(sample_events[1])
    assert score > 0.8  # Should be high due to many views
    
    # Test medium view count
    score = recommendation_engine._calculate_popularity_score(sample_events[0])
    assert 0.3 < score < 0.8  # Should be medium
    
    # Test low view count
    score = recommendation_engine._calculate_popularity_score(sample_events[2])
    assert score < 0.3  # Should be low due to few views

def test_get_personalized_recommendations(recommendation_engine, sample_events, sample_preferences):
    """Test getting personalized recommendations."""
    recommendations = recommendation_engine.get_personalized_recommendations(
        preferences=sample_preferences,
        events=sample_events,
        limit=2
    )
    
    assert len(recommendations) == 2
    assert recommendations[0].id == 1  # Rock concert should be first
    assert recommendations[1].id == 3  # Yoga should be second
    # Tech conference should not be included due to excluded category

def test_get_trending_recommendations(recommendation_engine, sample_events):
    """Test getting trending recommendations."""
    trending = recommendation_engine.get_trending_recommendations(
        events=sample_events,
        timeframe_days=7,
        limit=2
    )
    
    assert len(trending) == 2
    assert trending[0].id == 1  # Rock concert should be first due to high views and soon
    assert trending[1].id == 3  # Yoga should be second due to being very soon 