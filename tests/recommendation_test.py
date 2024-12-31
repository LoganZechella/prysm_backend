import pytest
import pandas as pd
from datetime import datetime, timezone
from app.models.profile import UserProfile, LocationPreference, EventPreference
from app.recommendation.engine import RecommendationEngine

@pytest.fixture
def sample_events_df():
    events_data = [
        {
            'event_id': '1',
            'title': 'Tech Meetup',
            'description': 'A tech meetup about AI',
            'start_datetime': '2024-03-01T19:00:00Z',
            'location': {
                'venue_name': 'Tech Hub',
                'coordinates': {
                    'lat': 37.7749,
                    'lng': -122.4194
                }
            },
            'categories': ['Tech', 'Networking'],
            'price_info': {
                'min_price': 0,
                'max_price': 0
            }
        },
        {
            'event_id': '2',
            'title': 'Jazz Concert',
            'description': 'Live jazz music',
            'start_datetime': '2024-03-02T20:00:00Z',
            'location': {
                'venue_name': 'Music Hall',
                'coordinates': {
                    'lat': 37.7848,
                    'lng': -122.4294
                }
            },
            'categories': ['Music', 'Live Entertainment'],
            'price_info': {
                'min_price': 50,
                'max_price': 100
            }
        }
    ]
    return pd.DataFrame(events_data)

@pytest.fixture
def sample_profile():
    return UserProfile(
        user_id="test123",
        name="Test User",
        email="test@example.com",
        location_preference=LocationPreference(
            city="San Francisco",
            state="CA",
            country="USA",
            max_distance_km=10.0
        ),
        event_preferences=EventPreference(
            categories=["Tech", "Music"],
            min_price=0,
            max_price=75,
            preferred_days=["Friday", "Saturday"],
            preferred_times=["evening"]
        ),
        interests=["Technology", "AI", "Jazz"],
        excluded_categories=["Sports"]
    )

def test_recommendation_engine_init(sample_events_df):
    """Test recommendation engine initialization."""
    engine = RecommendationEngine(sample_events_df)
    assert engine.events_df is not None
    assert len(engine.events_df) == 2

def test_calculate_distance(sample_events_df):
    """Test distance calculation."""
    engine = RecommendationEngine(sample_events_df)
    
    # Test with valid coordinates
    user_location = {'lat': 37.7749, 'lng': -122.4194}
    event_location = {'lat': 37.7848, 'lng': -122.4294}
    distance = engine._calculate_distance(user_location, event_location)
    assert distance > 0
    assert distance < 2  # Should be roughly 1.3 km
    
    # Test with invalid coordinates
    invalid_location = {'lat': 37.7749}
    distance = engine._calculate_distance(user_location, invalid_location)
    assert distance == float('inf')

def test_is_time_match(sample_events_df):
    """Test time preference matching."""
    engine = RecommendationEngine(sample_events_df)
    
    # Test evening time
    evening_time = datetime(2024, 3, 1, 19, 0, tzinfo=timezone.utc)
    assert engine._is_time_match(evening_time, ['evening'])
    assert not engine._is_time_match(evening_time, ['morning'])
    
    # Test morning time
    morning_time = datetime(2024, 3, 1, 9, 0, tzinfo=timezone.utc)
    assert engine._is_time_match(morning_time, ['morning'])
    assert not engine._is_time_match(morning_time, ['evening'])

def test_calculate_category_match_score(sample_events_df):
    """Test category matching."""
    engine = RecommendationEngine(sample_events_df)
    
    # Test perfect match
    score = engine._calculate_category_match_score(
        ['Tech', 'AI'],
        ['Technology', 'AI'],
        ['Tech']
    )
    assert score > 0.5
    
    # Test partial match
    score = engine._calculate_category_match_score(
        ['Tech', 'AI'],
        ['Music'],
        ['Sports']
    )
    assert score == 0
    
    # Test empty categories
    score = engine._calculate_category_match_score(
        [],
        ['Tech'],
        ['Tech']
    )
    assert score == 0

def test_get_recommendations(sample_events_df, sample_profile):
    """Test getting recommendations."""
    engine = RecommendationEngine(sample_events_df)
    recommendations = engine.get_recommendations(sample_profile)
    
    assert isinstance(recommendations, list)
    assert len(recommendations) > 0
    
    # First recommendation should be Tech Meetup (better category match)
    assert recommendations[0]['title'] == 'Tech Meetup'
    
    # Verify recommendation structure
    first_rec = recommendations[0]
    assert 'event_id' in first_rec
    assert 'title' in first_rec
    assert 'description' in first_rec
    assert 'start_datetime' in first_rec
    assert 'location' in first_rec
    assert 'categories' in first_rec
    assert 'price_info' in first_rec
    assert 'distance_km' in first_rec
    assert 'match_score' in first_rec

def test_get_recommendations_with_exclusions(sample_events_df, sample_profile):
    """Test recommendations with excluded categories."""
    # Add a sports event
    sports_event = {
        'event_id': '3',
        'title': 'Basketball Game',
        'description': 'NBA game',
        'start_datetime': '2024-03-01T19:00:00Z',
        'location': {
            'venue_name': 'Stadium',
            'coordinates': {
                'lat': 37.7749,
                'lng': -122.4194
            }
        },
        'categories': ['Sports', 'Entertainment'],
        'price_info': {
            'min_price': 30,
            'max_price': 200
        }
    }
    events_df = pd.concat([sample_events_df, pd.DataFrame([sports_event])], ignore_index=True)
    
    engine = RecommendationEngine(events_df)
    recommendations = engine.get_recommendations(sample_profile)
    
    # Verify that sports event is not in recommendations
    event_titles = [rec['title'] for rec in recommendations]
    assert 'Basketball Game' not in event_titles 