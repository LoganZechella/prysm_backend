"""
Tests for event processing service.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from app.services.event_processing import EventProcessor
from app.schemas.validation import SchemaVersion

@pytest.fixture
def event_processor():
    return EventProcessor()

@pytest.fixture
def valid_event_data():
    return {
        "title": "Summer Music Festival 2024",
        "description": "Annual summer music festival featuring top artists",
        "start_date": datetime.now() + timedelta(days=30),
        "end_date": datetime.now() + timedelta(days=32),
        "categories": [{"name": "Music Festival", "confidence": 0.95}],
        "location": {
            "name": "Central Park",
            "address": "Central Park West",
            "city": "New York",
            "state": "NY",
            "country": "USA",
            "postal_code": "10024",
            "latitude": 40.7829,
            "longitude": -73.9654
        },
        "prices": [{
            "amount": 49.99,
            "currency": "USD",
            "tier": "General Admission"
        }],
        "source": "eventbrite",
        "external_id": "evt-123456",
        "metadata": {}
    }

@pytest.fixture
def mock_nlp_response():
    return {
        'entities': [
            {
                'name': 'Music Festival',
                'type': 'EVENT',
                'metadata': {}
            },
            {
                'name': 'Summer',
                'type': 'OTHER',
                'metadata': {}
            }
        ],
        'sentiment': {
            'document_sentiment': {
                'score': 0.8,
                'magnitude': 0.9
            }
        },
        'language': 'en'
    }

@patch('app.services.nlp_service.NLPService')
def test_process_event_success(mock_nlp_service, event_processor, valid_event_data, mock_nlp_response):
    # Setup mock
    mock_nlp_instance = Mock()
    mock_nlp_instance.analyze_text.return_value = mock_nlp_response
    mock_nlp_service.return_value = mock_nlp_instance
    
    # Process event
    result = event_processor.process_event(valid_event_data)
    
    # Verify validation
    assert result['title'] == "Summer Music Festival 2024"
    assert result['schema_version'] == SchemaVersion.V2
    
    # Verify NLP enrichment
    assert 'entities' in result['metadata']
    assert 'sentiment' in result['metadata']
    assert 'music festival' in result['tags']
    
    # Verify NLP service was called
    mock_nlp_instance.analyze_text.assert_called_once()

@patch('app.services.nlp_service.NLPService')
def test_process_event_invalid_data(mock_nlp_service, event_processor):
    invalid_data = {
        "title": "",  # Invalid: empty title
        "description": "Test event"
    }
    
    with pytest.raises(ValueError, match="Validation error"):
        event_processor.process_event(invalid_data)

@patch('app.services.nlp_service.NLPService')
def test_extract_topics(mock_nlp_service, event_processor, mock_nlp_response):
    # Setup mock
    mock_nlp_instance = Mock()
    mock_nlp_instance.analyze_text.return_value = mock_nlp_response
    mock_nlp_service.return_value = mock_nlp_instance
    
    topics = event_processor.extract_topics(
        "Summer Music Festival",
        "A great music festival in summer"
    )
    
    assert "music festival" in topics
    mock_nlp_instance.analyze_text.assert_called_once()

def test_calculate_event_scores(event_processor, valid_event_data):
    user_preferences = {
        "preferred_categories": ["Music Festival", "Concert"],
        "excluded_categories": ["Sports"],
        "min_price": 0,
        "max_price": 100,
        "preferred_location": {
            "latitude": 40.7829,
            "longitude": -73.9654
        },
        "max_distance": 50
    }
    
    scores = event_processor.calculate_event_scores(valid_event_data, user_preferences)
    
    assert 0 <= scores["total_score"] <= 1
    assert "category_score" in scores
    assert "price_score" in scores
    assert "distance_score" in scores
    assert "sentiment_score" in scores

def test_calculate_event_scores_excluded_category(event_processor, valid_event_data):
    user_preferences = {
        "preferred_categories": ["Concert"],
        "excluded_categories": ["Music Festival"],
        "min_price": 0,
        "max_price": 100
    }
    
    scores = event_processor.calculate_event_scores(valid_event_data, user_preferences)
    
    assert scores["category_score"] == 0.0
    assert scores["total_score"] < 0.5  # Should be low due to category exclusion

@patch('app.services.nlp_service.NLPService')
def test_process_event_retry_on_failure(mock_nlp_service, event_processor, valid_event_data):
    # Setup mock to fail twice then succeed
    mock_nlp_instance = Mock()
    mock_nlp_instance.analyze_text.side_effect = [
        Exception("API Error"),
        Exception("API Error"),
        {
            'entities': [],
            'sentiment': {'document_sentiment': {'score': 0, 'magnitude': 0}},
            'language': 'en'
        }
    ]
    mock_nlp_service.return_value = mock_nlp_instance
    
    # Should succeed on third try
    result = event_processor.process_event(valid_event_data)
    
    assert result is not None
    assert mock_nlp_instance.analyze_text.call_count == 3

@patch('app.services.nlp_service.NLPService')
def test_process_event_circuit_breaker(mock_nlp_service, event_processor, valid_event_data):
    # Setup mock to always fail
    mock_nlp_instance = Mock()
    mock_nlp_instance.analyze_text.side_effect = Exception("API Error")
    mock_nlp_service.return_value = mock_nlp_instance
    
    # Should fail after max retries and open circuit breaker
    with pytest.raises(Exception):
        event_processor.process_event(valid_event_data)
    
    # Next call should fail fast with circuit breaker open
    with pytest.raises(Exception, match="Circuit breaker event_processing is OPEN"):
        event_processor.process_event(valid_event_data) 