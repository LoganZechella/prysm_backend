"""
Tests for the NLP service.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from google.api_core import exceptions
from app.services.nlp_service import NLPService
from app.monitoring.performance import PerformanceMonitor

@pytest.fixture
def mock_language_client():
    with patch('google.cloud.language_v2.LanguageServiceClient') as mock:
        yield mock

@pytest.fixture
def nlp_service():
    service = NLPService()
    service._client = None  # Reset client for testing
    return service

def test_singleton_instance():
    service1 = NLPService()
    service2 = NLPService()
    assert service1 is service2

def test_client_initialization(mock_language_client):
    with patch('app.config.google_cloud.get_credentials_path', return_value='/fake/path'):
        service = NLPService()
        client = service.get_client()
        assert client is not None
        mock_language_client.from_service_account_file.assert_called_once_with('/fake/path')

def test_client_initialization_failure():
    with patch('app.config.google_cloud.get_credentials_path', return_value=None):
        service = NLPService()
        with pytest.raises(ValueError, match="Invalid or missing GCP credentials"):
            service.get_client()

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=True)
def test_analyze_text_success(mock_acquire, nlp_service):
    # Mock successful API responses
    mock_client = MagicMock()
    mock_entity_response = MagicMock(
        entities=[],
        language_code='en'
    )
    mock_sentiment_response = MagicMock(
        document_sentiment=MagicMock(score=0.5, magnitude=0.8),
        sentences=[]
    )
    
    mock_client.analyze_entities.return_value = mock_entity_response
    mock_client.analyze_sentiment.return_value = mock_sentiment_response
    
    nlp_service._client = mock_client
    
    result = nlp_service.analyze_text("Test text")
    
    assert result['language'] == 'en'
    assert result['sentiment']['document_sentiment']['score'] == 0.5
    assert result['sentiment']['document_sentiment']['magnitude'] == 0.8
    assert isinstance(result['entities'], list)
    assert 'analyzed_at' in result

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=True)
def test_analyze_text_retry_on_timeout(mock_acquire, nlp_service):
    mock_client = MagicMock()
    mock_client.analyze_entities.side_effect = [
        TimeoutError("Timeout"),
        MagicMock(entities=[], language_code='en')
    ]
    mock_client.analyze_sentiment.return_value = MagicMock(
        document_sentiment=MagicMock(score=0.5, magnitude=0.8),
        sentences=[]
    )
    
    nlp_service._client = mock_client
    
    result = nlp_service.analyze_text("Test text")
    
    assert mock_client.analyze_entities.call_count == 2
    assert result['language'] == 'en'

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=True)
def test_analyze_text_circuit_breaker(mock_acquire, nlp_service):
    mock_client = MagicMock()
    mock_client.analyze_entities.side_effect = exceptions.ServiceUnavailable("Service down")
    
    nlp_service._client = mock_client
    
    # Should fail after max retries and open circuit breaker
    with pytest.raises(exceptions.ServiceUnavailable):
        nlp_service.analyze_text("Test text")
    
    # Next call should fail fast with circuit breaker open
    with pytest.raises(Exception, match="Circuit breaker nlp_api is OPEN"):
        nlp_service.analyze_text("Test text")

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=False)
def test_rate_limit_exceeded(mock_acquire, nlp_service):
    with pytest.raises(Exception, match="Rate limit exceeded"):
        nlp_service.analyze_text("Test text")

def test_cleanup(nlp_service):
    mock_client = MagicMock()
    nlp_service._client = mock_client
    
    nlp_service.cleanup()
    
    assert nlp_service._client is None
    mock_client.transport.close.assert_called_once() 