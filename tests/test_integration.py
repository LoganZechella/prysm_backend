import pytest
from datetime import datetime, timedelta
from app.utils.schema import UserPreferences

def test_recommendation_pipeline(test_events, recommendation_engine, data_quality_checker):
    """Test the full recommendation pipeline with generated test data"""
    
    # Validate event data quality
    quality_stats = data_quality_checker.validate_events(test_events)
    
    # Assert all events are valid
    assert quality_stats['valid_events'] == quality_stats['total_events'], \
        f"Invalid events found: {quality_stats['error_types']}"
    
    # Create test user preferences
    user_prefs = UserPreferences(
        user_id="test_user",
        preferred_categories=["music", "art"],
        price_preferences=["free", "budget", "medium"],
        preferred_location={"lat": 37.7749, "lng": -122.4194},  # San Francisco
        preferred_radius=25.0,
        min_price=0,
        max_price=200
    )
    
    # Get recommendations
    recommendations = recommendation_engine.get_personalized_recommendations(
        user_preferences=user_prefs,
        available_events=test_events,
        n=10
    )
    
    # Verify recommendations
    assert len(recommendations) > 0, "No recommendations returned"
    
    # Check that recommendations match user preferences
    for event, score in recommendations:
        # Verify score is between 0 and 1
        assert 0 <= score <= 1, f"Invalid recommendation score: {score}"
        
        # Verify price is within range
        assert event.price_info.min_price <= user_prefs.max_price, \
            f"Event price {event.price_info.min_price} exceeds max price {user_prefs.max_price}"
        
        # Verify at least one category matches
        has_matching_category = any(
            cat in user_prefs.preferred_categories 
            for cat in event.categories
        )
        assert has_matching_category, \
            f"Event categories {event.categories} don't match preferences {user_prefs.preferred_categories}"

def test_event_scoring(test_events, recommendation_engine):
    """Test event scoring based on user preferences"""
    
    # Get first event
    event = test_events[0]
    
    # Create test preferences matching the event
    user_prefs = UserPreferences(
        user_id="test_user",
        preferred_categories=event.categories,
        price_preferences=[event.price_info.price_tier],
        preferred_location=event.location.coordinates,
        preferred_radius=10.0
    )
    
    # Calculate score
    score = recommendation_engine._calculate_event_score(event, user_prefs)
    
    # Verify score
    assert 0 <= score <= 1, f"Invalid event score: {score}"
    # Perfect match should have high score
    assert score > 0.8, f"Perfect match should have high score, got {score}"

def test_location_based_recommendations(test_events, recommendation_engine):
    """Test location-based filtering in recommendations"""
    
    # Create user preferences with SF location and small radius
    user_prefs = UserPreferences(
        user_id="test_user",
        preferred_location={"lat": 37.7749, "lng": -122.4194},  # San Francisco
        preferred_radius=5.0  # Small radius to test location filtering
    )
    
    recommendations = recommendation_engine.get_personalized_recommendations(
        user_preferences=user_prefs,
        available_events=test_events,
        n=10
    )
    
    # Verify recommendations are within radius
    for event, _ in recommendations:
        # Skip if location coordinates are not available
        if not user_prefs.preferred_location or not event.location.coordinates:
            continue
            
        distance = recommendation_engine._haversine_distance(
            user_prefs.preferred_location['lat'],
            user_prefs.preferred_location['lng'],
            event.location.coordinates['lat'],
            event.location.coordinates['lng']
        )
        assert distance <= user_prefs.preferred_radius, \
            f"Event location {event.location.coordinates} is outside radius {user_prefs.preferred_radius}km"

def test_price_based_recommendations(test_events, recommendation_engine):
    """Test price-based filtering in recommendations"""
    
    # Create user preferences with strict price range
    user_prefs = UserPreferences(
        user_id="test_user",
        price_preferences=["free", "budget"],
        min_price=0,
        max_price=50  # Only free and budget events
    )
    
    recommendations = recommendation_engine.get_personalized_recommendations(
        user_preferences=user_prefs,
        available_events=test_events,
        n=10
    )
    
    # Verify recommendations are within price range
    for event, _ in recommendations:
        assert event.price_info.max_price <= user_prefs.max_price, \
            f"Event price {event.price_info.max_price} exceeds max price {user_prefs.max_price}"
        assert event.price_info.price_tier in user_prefs.price_preferences, \
            f"Event price tier {event.price_info.price_tier} not in preferences {user_prefs.price_preferences}" 