"""Tests for the ML-based recommendation model."""
import pytest
from datetime import datetime, timedelta
import numpy as np
import json
import torch
from app.recommendation.models.recommendation_model import RecommendationModel, EventScoringNetwork
from app.models.event import EventModel
from app.models.traits import Traits
import torch.nn as nn

@pytest.fixture
def sample_categories():
    """Create sample categories for testing."""
    return [
        ["technology", "software", "artificial-intelligence"],
        ["technology", "data-science", "machine-learning"],
        ["business", "entrepreneurship", "startup"],
        ["creative", "design", "user-experience"],
        ["technology", "cloud", "devops"],
        ["business", "marketing", "digital"],
        ["creative", "art", "digital-art"],
        ["technology", "security", "cybersecurity"],
        ["business", "finance", "blockchain"],
        ["creative", "music", "production"]
    ]

@pytest.fixture
def recommendation_model(sample_categories):
    """Create recommendation model with initialized embeddings."""
    model = RecommendationModel()
    
    # Initialize category embeddings
    model.category_embedder.fit(sample_categories)
    
    # Initialize neural network weights properly
    for m in model.scoring_network.modules():
        if isinstance(m, nn.Linear):
            nn.init.kaiming_normal_(m.weight, nonlinearity='relu')
            if m.bias is not None:
                if m == model.scoring_network.high_score:
                    nn.init.constant_(m.bias, 0.1)
                elif m == model.scoring_network.low_score:
                    nn.init.constant_(m.bias, -0.5)
                else:
                    nn.init.zeros_(m.bias)
    
    return model

@pytest.fixture
def sample_user_traits():
    """Create sample user traits for testing."""
    now = datetime.utcnow()
    return Traits(
        user_id="test_user",
        music_traits={
            "genres": ["rock", "jazz", "classical"],
            "artists": {
                "short_term": [{"name": "Artist1", "popularity": 0.8}],
                "medium_term": [{"name": "Artist2", "popularity": 0.7}]
            }
        },
        social_traits={
            "metrics": json.dumps({
                "engagement_level": 0.7,
                "preferred_group_size": 0.5,
                "social_activity_score": 0.6
            })
        },
        behavior_traits={
            "patterns": json.dumps({
                "hourly_distribution": [0.1] * 24,  # Uniform distribution
                "daily_distribution": [0.14] * 7,   # Uniform distribution
                "activity_clusters": [[9, 10, 11], [14, 15, 16]]  # Morning and afternoon
            }),
            "event_history": [
                {
                    "timestamp": (now - timedelta(days=30)).isoformat(),
                    "technical_level": 0.3,
                    "attendee_count": 30,
                    "categories": ["technology", "beginner"]
                },
                {
                    "timestamp": (now - timedelta(days=20)).isoformat(),
                    "technical_level": 0.4,
                    "attendee_count": 40,
                    "categories": ["technology", "intermediate"]
                },
                {
                    "timestamp": (now - timedelta(days=10)).isoformat(),
                    "technical_level": 0.5,
                    "attendee_count": 50,
                    "categories": ["technology", "advanced"]
                }
            ]
        },
        professional_traits={
            "interests": json.dumps({
                "technology": 0.8,
                "business": 0.6,
                "creative": 0.4
            }),
            "interests_text": "Software development, machine learning, and data science",
            "background": "Senior software engineer with 5 years of experience in ML/AI",
            "categories": ["technology", "software", "artificial-intelligence"],
            "skills": json.dumps({
                "technical": {
                    "programming": 0.9,
                    "data_science": 0.7
                },
                "business": {
                    "management": 0.5
                }
            }),
            "industries": json.dumps({
                "technology": {
                    "software": 0.8,
                    "internet": 0.7
                }
            })
        },
        last_updated_at=now,
        next_update_at=now + timedelta(days=7)
    )

@pytest.fixture
def sample_events():
    """Create sample events for testing."""
    now = datetime.utcnow()
    return [
        EventModel(
            id=1,
            platform_id="event1",
            title="Tech Conference 2024",
            description="Annual technology conference with workshops on AI and ML",
            start_datetime=now + timedelta(days=7),
            end_datetime=now + timedelta(days=7, hours=8),
            url="https://example.com/event1",
            venue_name="Tech Center",
            venue_lat=37.7749,
            venue_lon=-122.4194,
            venue_city="San Francisco",
            venue_state="CA",
            venue_country="USA",
            organizer_id="org1",
            organizer_name="Tech Events Inc",
            platform="eventbrite",
            is_online=False,
            rsvp_count=300,
            price_info={
                "currency": "USD",
                "min_price": 500.0,
                "max_price": 1000.0
            },
            categories=["technology", "education", "networking"],
            image_url="https://example.com/event1.jpg"
        ),
        EventModel(
            id=2,
            platform_id="event2",
            title="Local Networking Meetup",
            description="Casual networking event for professionals",
            start_datetime=now + timedelta(days=3),
            end_datetime=now + timedelta(days=3, hours=2),
            url="https://example.com/event2",
            venue_name="Coworking Space",
            venue_lat=37.7833,
            venue_lon=-122.4167,
            venue_city="San Francisco",
            venue_state="CA",
            venue_country="USA",
            organizer_id="org2",
            organizer_name="SF Professionals",
            platform="meetup",
            is_online=False,
            rsvp_count=40,
            price_info={
                "currency": "USD",
                "min_price": 0.0,
                "max_price": 0.0
            },
            categories=["networking", "social", "business"],
            image_url="https://example.com/event2.jpg"
        )
    ]

def test_score_events(recommendation_model, sample_user_traits, sample_events):
    """Test event scoring functionality."""
    scored_events = recommendation_model.score_events(sample_events, sample_user_traits)
    assert len(scored_events) == len(sample_events)
    for event in scored_events:
        assert 0 <= event["score"] <= 1
        assert "explanation" in event

def test_content_match(recommendation_model, sample_user_traits, sample_events):
    """Test content matching calculation."""
    event = sample_events[0]
    score = recommendation_model._calculate_content_match(event, sample_user_traits)
    assert 0 <= score <= 1

def test_social_match(recommendation_model, sample_user_traits, sample_events):
    """Test social matching calculation."""
    event = sample_events[0]
    score = recommendation_model._calculate_social_match(event, sample_user_traits)
    assert 0 <= score <= 1

def test_temporal_match(recommendation_model, sample_user_traits, sample_events):
    """Test temporal matching calculation."""
    event = sample_events[0]
    score = recommendation_model._calculate_temporal_match(event, sample_user_traits)
    assert 0 <= score <= 1

def test_contextual_match(recommendation_model, sample_user_traits, sample_events):
    """Test contextual matching calculation."""
    event = sample_events[0]
    score = recommendation_model._calculate_contextual_match(event, sample_user_traits)
    assert 0 <= score <= 1

def test_quality_score(recommendation_model, sample_events):
    """Test event quality scoring."""
    event = sample_events[0]
    score = recommendation_model._calculate_quality_score(event)
    assert 0 <= score <= 1

def test_vector_extraction(recommendation_model, sample_user_traits, sample_events):
    """Test feature vector extraction."""
    # Test that we can extract features from both user traits and events
    content_score = recommendation_model._calculate_content_match(sample_events[0], sample_user_traits)
    social_score = recommendation_model._calculate_social_match(sample_events[0], sample_user_traits)
    temporal_score = recommendation_model._calculate_temporal_match(sample_events[0], sample_user_traits)
    contextual_score = recommendation_model._calculate_contextual_match(sample_events[0], sample_user_traits)
    quality_score = recommendation_model._calculate_quality_score(sample_events[0])
    
    assert 0 <= content_score <= 1
    assert 0 <= social_score <= 1
    assert 0 <= temporal_score <= 1
    assert 0 <= contextual_score <= 1
    assert 0 <= quality_score <= 1

def test_explanation_generation(recommendation_model, sample_user_traits, sample_events):
    """Test explanation generation for scores."""
    event = sample_events[0]
    explanation = recommendation_model._generate_score_explanation(event, 0.8, {
        "content_match": 0.9,
        "social_match": 0.7,
        "temporal_match": 0.8,
        "contextual_match": 0.6,
        "quality_score": 0.9
    })
    assert isinstance(explanation, str)
    assert len(explanation) > 0

def test_neural_scoring(recommendation_model, sample_user_traits, sample_events):
    """Test neural network scoring functionality."""
    # Test network initialization
    assert isinstance(recommendation_model.scoring_network, EventScoringNetwork)
    assert recommendation_model.scoring_network.training is False  # Should be in eval mode
    
    # Test scoring
    component_scores = {
        'content_match': 0.8,
        'social_match': 0.7,
        'temporal_match': 0.6,
        'contextual_match': 0.7,
        'quality_score': 0.9
    }
    
    score = recommendation_model._neural_score(component_scores)
    assert isinstance(score, float)
    assert 0 <= score <= 1
    assert score > 0.6  # Should be high for good scores
    
    # Test with edge cases
    edge_scores = {
        'content_match': 0.0,
        'social_match': 0.0,
        'temporal_match': 0.0,
        'contextual_match': 0.0,
        'quality_score': 0.0
    }
    low_score = recommendation_model._neural_score(edge_scores)
    assert 0 <= low_score <= 0.4  # Should be low but not necessarily 0
    
    edge_scores = {k: 1.0 for k in edge_scores}
    high_score = recommendation_model._neural_score(edge_scores)
    assert 0.6 <= high_score <= 1.0  # Should be high but not necessarily 1

def test_text_similarity(recommendation_model, sample_user_traits, sample_events):
    """Test text similarity computation."""
    text1 = "Machine learning and artificial intelligence conference"
    text2 = "Advanced AI and deep learning workshop"
    
    similarity = recommendation_model._compute_text_similarity(text1, text2)
    assert isinstance(similarity, float)
    assert 0 <= similarity <= 1  # Normalized similarity
    assert similarity > 0.6  # Should be high for similar topics
    
    # Test with dissimilar texts
    text3 = "Cooking class for beginners"
    similarity = recommendation_model._compute_text_similarity(text1, text3)
    assert similarity < 0.4  # Should be low for different topics

def test_category_similarity(recommendation_model, sample_user_traits, sample_events):
    """Test category similarity computation."""
    cats1 = ["technology", "artificial-intelligence", "machine-learning"]
    cats2 = ["technology", "software", "data-science"]
    
    similarity = recommendation_model._compute_category_similarity(cats1, cats2)
    assert isinstance(similarity, float)
    assert 0 <= similarity <= 1  # Normalized similarity
    assert similarity > 0.6  # Should be high for related categories
    
    # Test with dissimilar categories
    cats3 = ["food", "cooking", "lifestyle"]
    similarity = recommendation_model._compute_category_similarity(cats1, cats3)
    assert similarity < 0.4  # Should be low for different categories

def test_progression_pattern_analysis(recommendation_model, sample_user_traits, sample_events):
    """Test user progression pattern analysis."""
    event = sample_events[0]
    event_history = sample_user_traits.behavior_traits['event_history']
    
    # Set technical level for test event
    event.technical_level = 0.6
    
    score = recommendation_model._analyze_progression_pattern(event_history, event)
    assert isinstance(score, float)
    assert 0 <= score <= 1
    
    # Test with empty history
    score = recommendation_model._analyze_progression_pattern([], event)
    assert score == 0.5  # Should return neutral score
    
    # Test progression detection
    # Create event history with clear progression
    history_with_progression = [
        {
            'timestamp': (datetime.utcnow() - timedelta(days=30)).isoformat(),
            'technical_level': 0.3,
            'attendee_count': 30,
            'categories': ['technology', 'beginner']
        },
        {
            'timestamp': (datetime.utcnow() - timedelta(days=20)).isoformat(),
            'technical_level': 0.5,
            'attendee_count': 50,
            'categories': ['technology', 'intermediate']
        },
        {
            'timestamp': (datetime.utcnow() - timedelta(days=10)).isoformat(),
            'technical_level': 0.7,
            'attendee_count': 70,
            'categories': ['technology', 'advanced']
        }
    ]
    
    # Create advanced event that follows the progression
    event.technical_level = 0.8
    event.rsvp_count = 90
    event.categories = ['technology', 'advanced', 'artificial-intelligence']
    
    score = recommendation_model._analyze_progression_pattern(history_with_progression, event)
    assert score > 0.7  # Should detect strong positive progression

def test_event_frequency_analysis(recommendation_model, sample_user_traits, sample_events):
    """Test event frequency pattern analysis."""
    event = sample_events[0]
    event_history = sample_user_traits.behavior_traits['event_history']
    
    score = recommendation_model._analyze_event_frequency(event_history, event)
    assert isinstance(score, float)
    assert 0 <= score <= 1
    
    # Test with empty history
    score = recommendation_model._analyze_event_frequency([], event)
    assert score == 0.5  # Should return neutral score
    
    # Test frequency pattern matching
    # Create event at expected interval
    last_event_time = datetime.fromisoformat(event_history[-1]['timestamp'])
    avg_interval = timedelta(days=10)  # Based on sample data
    expected_event = EventModel(
        id=4,
        title="Test Event",
        start_datetime=last_event_time + avg_interval
    )
    score = recommendation_model._analyze_event_frequency(event_history, expected_event)
    assert score > 0.7  # Should highly match the frequency pattern

def test_enhanced_explanation_generation(recommendation_model, sample_user_traits, sample_events):
    """Test enhanced explanation generation with multiple factors."""
    event = sample_events[0]
    
    # Test with high scores
    explanation = recommendation_model._generate_score_explanation(event, 0.9, {
        "content_match": 0.9,
        "social_match": 0.8,
        "temporal_match": 0.9,
        "contextual_match": 0.8,
        "quality_score": 0.9
    })
    assert isinstance(explanation, str)
    assert "excellent match" in explanation.lower()
    assert "perfectly fits" in explanation.lower()
    
    # Test with mixed scores
    explanation = recommendation_model._generate_score_explanation(event, 0.6, {
        "content_match": 0.7,
        "social_match": 0.3,
        "temporal_match": 0.8,
        "contextual_match": 0.4,
        "quality_score": 0.6
    })
    assert isinstance(explanation, str)
    assert any(warning in explanation.lower() for warning in [
        "comfort zone",
        "may require adjustments",
        "outside your usual"
    ])  # Should mention social mismatch
    assert "aligns with your usual event timing" in explanation.lower()  # Should mention good timing 