import pytest
from datetime import datetime, timedelta, time
import copy

from app.utils.schema import (
    Event, Location, PriceInfo, SourceInfo,
    ImageAnalysis, UserPreferences
)
from app.utils.recommendation import RecommendationEngine

@pytest.fixture
def recommendation_engine():
    """Create a RecommendationEngine instance for testing"""
    return RecommendationEngine()

@pytest.fixture
def sample_events():
    """Create sample events for testing"""
    base_time = datetime.utcnow()
    
    events = [
        Event(
            event_id="event1",
            title="Rock Concert",
            description="A great rock concert",
            start_datetime=base_time,
            location=Location(
                venue_name="Concert Hall",
                address="123 Music St",
                city="San Francisco",
                state="CA",
                country="United States",
                coordinates={"lat": 37.7749, "lng": -122.4194}
            ),
            categories=["music", "concert", "rock"],
            price_info=PriceInfo(
                currency="USD",
                min_price=50.0,
                max_price=100.0,
                price_tier="medium"
            ),
            source=SourceInfo(
                platform="test",
                url="https://example.com/event1",
                last_updated=datetime.utcnow()
            ),
            image_analysis=[
                ImageAnalysis(
                    url="https://example.com/image1.jpg",
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
        ),
        Event(
            event_id="event2",
            title="Jazz Night",
            description="Smooth jazz evening",
            start_datetime=base_time + timedelta(hours=2),
            location=Location(
                venue_name="Jazz Club",
                address="456 Jazz Ave",
                city="San Francisco",
                state="CA",
                country="United States",
                coordinates={"lat": 37.7848, "lng": -122.4294}
            ),
            categories=["music", "jazz", "nightlife"],
            price_info=PriceInfo(
                currency="USD",
                min_price=30.0,
                max_price=60.0,
                price_tier="medium"
            ),
            source=SourceInfo(
                platform="test",
                url="https://example.com/event2",
                last_updated=datetime.utcnow()
            ),
            image_analysis=[
                ImageAnalysis(
                    url="https://example.com/image2.jpg",
                    scene_classification={
                        "concert_hall": 0.8,
                        "stage": 0.8,
                        "crowd": 0.6,
                        "indoor": 0.7
                    },
                    crowd_density={"density": "medium", "count_estimate": "10-50", "confidence": 0.8},
                    objects=[{"name": "stage", "confidence": 0.9}],
                    safe_search={"adult": "UNLIKELY", "violence": "UNLIKELY", "racy": "POSSIBLE"}
                )
            ]
        ),
        Event(
            event_id="event3",
            title="Art Exhibition",
            description="Modern art showcase",
            start_datetime=base_time + timedelta(days=1),
            location=Location(
                venue_name="Art Gallery",
                address="789 Art Blvd",
                city="San Francisco",
                state="CA",
                country="United States",
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
                platform="test",
                url="https://example.com/event3",
                last_updated=datetime.utcnow()
            ),
            image_analysis=[
                ImageAnalysis(
                    url="https://example.com/image3.jpg",
                    scene_classification={
                        "art_gallery": 0.9,
                        "indoor": 0.8,
                        "crowd": 0.3,
                        "stage": 0.2
                    },
                    crowd_density={"density": "low", "count_estimate": "<10", "confidence": 0.9},
                    objects=[{"name": "painting", "confidence": 0.95}],
                    safe_search={"adult": "UNLIKELY", "violence": "UNLIKELY", "racy": "UNLIKELY"}
                )
            ]
        )
    ]
    
    # Add category hierarchies
    events[0].category_hierarchy = {
        "music": ["entertainment"],
        "concert": ["music", "entertainment"],
        "rock": ["music", "entertainment"]
    }
    events[1].category_hierarchy = {
        "music": ["entertainment"],
        "jazz": ["music", "entertainment"],
        "nightlife": ["entertainment"]
    }
    events[2].category_hierarchy = {
        "art": ["culture"],
        "exhibition": ["art", "culture"],
        "culture": []
    }
    
    return events

@pytest.fixture
def sample_preferences():
    """Create sample user preferences for testing"""
    return UserPreferences(
        user_id="user123",
        preferred_categories=["music", "jazz", "nightlife"],
        price_preferences=["budget", "medium"],
        preferred_location={"lat": 37.7749, "lng": -122.4194},
        preferred_radius=5.0,
        preferred_times=[
            (time(18, 0), time(23, 0)),
            (time(12, 0), time(15, 0))
        ],
        excluded_categories=["sports"],
        min_rating=4.0,
        indoor_outdoor_preference="indoor"
    )

def test_calculate_category_similarity(recommendation_engine, sample_events):
    """Test category similarity calculation"""
    event1, event2, event3 = sample_events
    
    # Similar music events should have high similarity
    similarity = recommendation_engine._calculate_category_similarity(event1, event2)
    assert similarity > 0.2  # Adjusted threshold for weighted similarity
    
    # Different category events should have low similarity
    similarity = recommendation_engine._calculate_category_similarity(event1, event3)
    assert similarity < 0.1  # Adjusted threshold for weighted similarity

def test_calculate_scene_similarity(sample_events):
    """Test scene similarity calculation between events"""
    engine = RecommendationEngine()
    
    # Test similarity between event1 (rock concert) and event2 (jazz night)
    # Both have high scores for concert_hall, stage, and indoor
    similarity = engine._calculate_scene_similarity(sample_events[0], sample_events[1])
    assert similarity > 0.8, "Concert venues should have high scene similarity"
    
    # Test similarity between event1 (rock concert) and event3 (art exhibition)
    # Different scenes (concert_hall vs art_gallery)
    similarity = engine._calculate_scene_similarity(sample_events[0], sample_events[2])
    assert similarity < 0.5, "Concert and art gallery should have low scene similarity"
    
    # Test similarity between event with no scene classification
    event_no_scene = copy.deepcopy(sample_events[0])
    event_no_scene.image_analysis[0].scene_classification = {}
    similarity = engine._calculate_scene_similarity(event_no_scene, sample_events[0])
    assert similarity == 0.0, "Event with no scene classification should have zero similarity"

def test_calculate_price_similarity(recommendation_engine, sample_events):
    """Test price similarity calculation"""
    event1, event2, event3 = sample_events
    
    # Same price tier events should have high similarity
    similarity = recommendation_engine._calculate_price_similarity(event1, event2)
    assert similarity > 0.8
    
    # Different price tier events should have lower similarity
    similarity = recommendation_engine._calculate_price_similarity(event1, event3)
    assert similarity < 0.8

def test_calculate_location_similarity(recommendation_engine, sample_events):
    """Test location similarity calculation"""
    event1, event2, event3 = sample_events
    
    # Nearby events should have high similarity
    similarity = recommendation_engine._calculate_location_similarity(event1, event2)
    assert similarity > 0.7  # Adjusted threshold for sigmoid function
    
    # More distant events should have moderate similarity
    similarity = recommendation_engine._calculate_location_similarity(event1, event3)
    assert 0.3 < similarity < 0.9  # Adjusted range for sigmoid function

def test_calculate_temporal_similarity(recommendation_engine, sample_events):
    """Test temporal similarity calculation"""
    event1, event2, event3 = sample_events
    
    # Events close in time should have high similarity
    similarity = recommendation_engine._calculate_temporal_similarity(event1, event2)
    assert similarity > 0.8
    
    # Events far apart in time should have lower similarity
    similarity = recommendation_engine._calculate_temporal_similarity(event1, event3)
    assert similarity < 0.5

def test_get_similar_events(recommendation_engine, sample_events):
    """Test getting similar events"""
    event1 = sample_events[0]
    similar_events = recommendation_engine.get_similar_events(event1, sample_events)
    
    # Should return n-1 events (excluding the input event)
    assert len(similar_events) == 2
    
    # Similar music event should be ranked higher than art event
    assert similar_events[0][0].event_id == "event2"
    assert similar_events[1][0].event_id == "event3"
    
    # Similarity scores should be between 0 and 1
    for _, score in similar_events:
        assert 0 <= score <= 1

def test_get_personalized_recommendations(recommendation_engine, sample_events, sample_preferences):
    """Test personalized recommendations"""
    recommendations = recommendation_engine.get_personalized_recommendations(
        sample_preferences,
        sample_events
    )
    
    # Should return all events, ranked by preference score
    assert len(recommendations) == 3
    
    # Jazz event should be ranked highest due to category and location match
    assert recommendations[0][0].event_id == "event2"
    
    # Scores should be between 0 and 1
    for _, score in recommendations:
        assert 0 <= score <= 1

def test_preference_score_calculation(recommendation_engine, sample_events, sample_preferences):
    """Test preference score calculation"""
    event1, event2, event3 = sample_events
    
    # Calculate scores
    score1 = recommendation_engine._calculate_preference_score(event1, sample_preferences)
    score2 = recommendation_engine._calculate_preference_score(event2, sample_preferences)
    score3 = recommendation_engine._calculate_preference_score(event3, sample_preferences)
    
    # Jazz event should have highest score due to category match
    assert score2 > score1
    assert score2 > score3
    
    # Art event should have lowest score due to category mismatch
    assert score3 < score1
    assert score3 < score2

def test_location_match(recommendation_engine, sample_events, sample_preferences):
    """Test location match calculation"""
    event1, event2, event3 = sample_events
    
    # Events within radius should have high match
    match1 = recommendation_engine._check_location_match(event1, sample_preferences)
    assert match1 > 0.8
    
    # Events outside radius should have lower match
    match3 = recommendation_engine._check_location_match(event3, sample_preferences)
    assert match3 < match1

def test_time_match(recommendation_engine, sample_events, sample_preferences):
    """Test time match calculation"""
    event = sample_events[0]
    
    # Event within preferred time slot
    event.start_datetime = datetime.combine(
        datetime.today(),
        time(19, 0)  # 7 PM
    )
    match = recommendation_engine._check_time_match(event, sample_preferences)
    assert match == 1.0
    
    # Event outside preferred time slots
    event.start_datetime = datetime.combine(
        datetime.today(),
        time(16, 0)  # 4 PM
    )
    match = recommendation_engine._check_time_match(event, sample_preferences)
    assert match == 0.0 

def test_get_scene_vector(sample_events):
    """Test conversion of scene classifications to feature vectors"""
    engine = RecommendationEngine()
    
    # Test vector creation for event1 (rock concert)
    scene_classification = sample_events[0].image_analysis[0].scene_classification
    vector = engine._get_scene_vector(scene_classification)
    
    # Check vector length matches scene categories
    assert len(vector) == len(engine.SCENE_CATEGORIES)
    
    # Check specific scene scores
    assert vector[engine.SCENE_CATEGORIES.index("concert_hall")] == 0.9
    assert vector[engine.SCENE_CATEGORIES.index("stage")] == 0.8
    assert vector[engine.SCENE_CATEGORIES.index("crowd")] == 0.7
    assert vector[engine.SCENE_CATEGORIES.index("indoor")] == 0.6
    
    # Test vector creation for empty scene classification
    empty_vector = engine._get_scene_vector({})
    assert len(empty_vector) == len(engine.SCENE_CATEGORIES)
    assert all(v == 0.0 for v in empty_vector) 