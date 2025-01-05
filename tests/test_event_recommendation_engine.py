import pytest
from app.services.recommendation import RecommendationService
from app.schemas import (
    Event,
    Location,
    PriceInfo,
    EventAttributes,
    SourceInfo,
    EventMetadata,
    UserPreferences
)
from datetime import datetime, time

@pytest.fixture
def recommendation_service():
    return RecommendationService()

@pytest.fixture
def sample_event():
    return Event(
        event_id="test-event-1",
        title="Test Event",
        description="A test event",
        start_datetime=datetime(2024, 3, 1, 14, 0),  # 2 PM
        location=Location(
            venue_name="Test Venue",
            coordinates={"lat": 37.7749, "lng": -122.4194},  # San Francisco
            city="San Francisco"
        ),
        categories=["Music", "Technology"],
        price_info=PriceInfo(
            min_price=50.0,
            max_price=100.0,
            price_tier="medium"
        ),
        attributes=EventAttributes(
            indoor_outdoor="indoor",
            age_restriction="all",
            accessibility_features=["wheelchair"]
        ),
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=datetime.now()
        ),
        metadata=EventMetadata(
            popularity_score=0.8
        )
    )

@pytest.fixture
def user_preferences():
    return UserPreferences(
        user_id="test-user",
        preferred_categories=["Music", "Technology"],
        price_preferences=["medium"],
        preferred_location={"lat": 37.7749, "lng": -122.4194},
        max_distance=50.0,  # Changed from preferred_radius
        preferred_times=[(time(9, 0), time(17, 0))],  # 9 AM to 5 PM
        min_price=0.0,
        max_price=150.0,
        preferred_days=["friday"],
        accessibility_requirements=["wheelchair"],
        indoor_outdoor_preference="indoor"
    )

def test_category_score(recommendation_service, sample_event, user_preferences):
    score = recommendation_service.calculate_category_score(sample_event, user_preferences)
    assert score == 1.0  # Perfect match
    
    # Test with no matching categories
    user_preferences.preferred_categories = ["Sports"]
    score = recommendation_service.calculate_category_score(sample_event, user_preferences)
    assert score == 0.0

def test_location_score(recommendation_service, sample_event, user_preferences):
    score = recommendation_service.calculate_location_score(sample_event, user_preferences)
    assert score == 1.0  # Same location
    
    # Test with location outside max distance
    user_preferences.preferred_location = {"lat": 40.7128, "lng": -74.0060}  # New York
    user_preferences.max_distance = 50.0  # Ensure max_distance is set
    score = recommendation_service.calculate_location_score(sample_event, user_preferences)
    assert score == 0.0

def test_price_score(recommendation_service, sample_event, user_preferences):
    score = recommendation_service.calculate_price_score(sample_event, user_preferences)
    assert score > 0.0  # Price within range and matching tier
    
    # Test with price outside range
    user_preferences.max_price = 40.0
    score = recommendation_service.calculate_price_score(sample_event, user_preferences)
    assert score == 0.0

def test_time_score(recommendation_service, sample_event, user_preferences):
    score = recommendation_service.calculate_time_score(sample_event, user_preferences)
    assert score > 0.0  # Time within preferred range
    
    # Test with time outside range
    user_preferences.preferred_times = [(time(18, 0), time(22, 0))]  # 6 PM to 10 PM
    score = recommendation_service.calculate_time_score(sample_event, user_preferences)
    assert score == 0.5  # Day matches but time doesn't

def test_attribute_score(recommendation_service, sample_event, user_preferences):
    score = recommendation_service.calculate_attribute_score(sample_event, user_preferences)
    assert score == 1.0  # All attributes match
    
    # Test with mismatched attributes
    user_preferences.indoor_outdoor_preference = "outdoor"
    score = recommendation_service.calculate_attribute_score(sample_event, user_preferences)
    assert score < 1.0

def test_get_personalized_recommendations(recommendation_service, sample_event, user_preferences):
    # Create a list of events with varying relevance
    events = [
        Event(
            event_id="test-event-2",
            title="Another Test Event",
            description="Another test event",
            start_datetime=datetime(2024, 3, 1, 20, 0),  # 8 PM
            location=Location(
                venue_name="Test Venue 2",
                coordinates={"lat": 37.7749, "lng": -122.4194},  # San Francisco
                city="San Francisco"
            ),
            categories=["Sports"],
            price_info=PriceInfo(
                min_price=200.0,
                max_price=300.0,
                price_tier="high"
            ),
            attributes=EventAttributes(
                indoor_outdoor="outdoor",
                age_restriction="18+",
                accessibility_features=[]
            ),
            source=SourceInfo(
                platform="test",
                url="http://test.com",
                last_updated=datetime.now()
            ),
            metadata=EventMetadata(
                popularity_score=0.5
            )
        )
    ]
    
    recommendations = recommendation_service.get_personalized_recommendations(
        user_preferences,
        events
    )
    
    assert len(recommendations) > 0
    # First event should be the sample_event as it matches better
    assert recommendations[0].event_id == sample_event.event_id 