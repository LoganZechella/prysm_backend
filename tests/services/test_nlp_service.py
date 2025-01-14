"""
Tests for the NLP service.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
import time
from google.cloud import language_v2
from google.api_core import exceptions

from app.services.nlp_service import NLPService, RateLimiter

@pytest.fixture
def mock_client():
    """Mock Google Cloud client"""
    with patch('google.cloud.language_v2.LanguageServiceClient') as mock:
        yield mock

@pytest.fixture
def nlp_service(mock_client):
    """Get NLP service instance with mocked client"""
    service = NLPService()
    service._client = mock_client
    return service

def test_rate_limiter():
    """Test rate limiter functionality"""
    limiter = RateLimiter(rate=60, burst_size=2)  # 1 per second, burst of 2
    
    # Should allow burst
    assert limiter.acquire()
    assert limiter.acquire()
    
    # Should deny when depleted
    assert not limiter.acquire()
    
    # Should allow after waiting
    time.sleep(1.1)  # Wait for token replenishment
    assert limiter.acquire()

def test_singleton_pattern():
    """Test NLP service singleton pattern"""
    service1 = NLPService()
    service2 = NLPService()
    assert service1 is service2

def test_analyze_text_success(nlp_service, mock_client):
    """Test successful text analysis"""
    # Mock responses
    mock_entity_response = Mock()
    mock_entity_response.entities = [
        Mock(
            name="Google",
            type_=language_v2.Entity.Type.ORGANIZATION,
            salience=0.8,
            metadata={},
            mentions=[
                Mock(
                    text=Mock(content="Google"),
                    type_=language_v2.EntityMention.Type.PROPER,
                    sentiment=Mock(score=0.2, magnitude=0.5)
                )
            ]
        )
    ]
    
    mock_sentiment_response = Mock()
    mock_sentiment_response.document_sentiment = Mock(score=0.4, magnitude=0.8)
    mock_sentiment_response.sentences = [
        Mock(
            text=Mock(content="This is great."),
            sentiment=Mock(score=0.4, magnitude=0.8)
        )
    ]
    
    mock_client.analyze_entities.return_value = mock_entity_response
    mock_client.analyze_sentiment.return_value = mock_sentiment_response
    
    result = nlp_service.analyze_text("Google is a great company")
    
    assert result['entities'][0]['name'] == "Google"
    assert result['entities'][0]['type'] == "ORGANIZATION"
    assert result['sentiment']['document_sentiment']['score'] == 0.4
    assert 'analyzed_at' in result

def test_analyze_text_api_error(nlp_service, mock_client):
    """Test handling of API errors"""
    mock_client.analyze_entities.side_effect = exceptions.DeadlineExceeded("Timeout")
    
    with pytest.raises(exceptions.DeadlineExceeded):
        nlp_service.analyze_text("Test text")

def test_rate_limit_exceeded(nlp_service):
    """Test rate limit enforcement"""
    # Override rate limiter for testing
    nlp_service.rate_limiter = RateLimiter(rate=1, burst_size=1)
    
    # First request should succeed
    nlp_service.analyze_text("Test 1")
    
    # Second immediate request should fail
    with pytest.raises(Exception, match="Rate limit exceeded"):
        nlp_service.analyze_text("Test 2")

def test_cache_behavior(nlp_service, mock_client):
    """Test caching behavior"""
    mock_client.analyze_entities.return_value = Mock(entities=[])
    mock_client.analyze_sentiment.return_value = Mock(
        document_sentiment=Mock(score=0, magnitude=0),
        sentences=[]
    )
    
    # First call should hit the API
    nlp_service.analyze_text("Test text")
    assert mock_client.analyze_entities.call_count == 1
    
    # Second call with same text should use cache
    nlp_service.analyze_text("Test text")
    assert mock_client.analyze_entities.call_count == 1  # Count shouldn't increase

def test_cleanup(nlp_service, mock_client):
    """Test cleanup behavior"""
    nlp_service.cleanup()
    assert nlp_service._client is None 