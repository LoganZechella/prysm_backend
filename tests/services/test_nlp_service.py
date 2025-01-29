"""
Tests for the NLP service.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, PropertyMock, call
from google.api_core import exceptions, retry
from google.oauth2 import service_account
from google.cloud.language_v2 import Document, EncodingType, Entity
from app.services.nlp_service import NLPService
from app.monitoring.performance import PerformanceMonitor
from app.utils.decorators import CircuitBreaker
import os
import time

@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset NLP service singleton between tests."""
    NLPService._instance = None
    yield

@pytest.fixture(autouse=True)
def reset_circuit_breakers():
    """Reset circuit breakers between tests."""
    with patch('app.utils.decorators._circuit_breakers', {}):
        yield

@pytest.fixture
def mock_settings():
    """Mock settings with test values."""
    with patch('app.config.google_cloud.settings') as mock:
        type(mock).GOOGLE_APPLICATION_CREDENTIALS = PropertyMock(return_value='/fake/path')
        mock.NLP_API_REGION = 'us-central1'
        mock.NLP_API_TIMEOUT = 30
        mock.NLP_REQUESTS_PER_MINUTE = 100
        mock.NLP_BURST_SIZE = 10
        mock.NLP_MAX_RETRIES = 3
        mock.NLP_INITIAL_RETRY_DELAY = 1.0
        mock.NLP_MAX_RETRY_DELAY = 60.0
        yield mock

@pytest.fixture
def mock_credentials():
    """Mock Google Cloud credentials."""
    mock_creds = Mock(spec=service_account.Credentials)
    with patch('google.oauth2.service_account.Credentials.from_service_account_file', return_value=mock_creds):
        yield mock_creds

@pytest.fixture
def mock_language_client(mock_credentials):
    """Mock Google Cloud Language client."""
    mock_client = MagicMock()
    mock_client._transport = MagicMock()
    with patch('google.cloud.language_v2.LanguageServiceClient', return_value=mock_client):
        yield mock_client

@pytest.fixture
def nlp_service(mock_settings, mock_credentials):
    """Create NLP service instance with mocked credentials."""
    with patch('os.path.exists', return_value=True):
        service = NLPService()
        service._client = None  # Reset client for testing
        yield service

def test_singleton_instance(mock_settings, mock_credentials):
    """Test NLP service singleton pattern."""
    with patch('os.path.exists', return_value=True):
        service1 = NLPService()
        service2 = NLPService()
        assert service1 is service2

def test_client_initialization(mock_language_client, mock_settings, mock_credentials):
    """Test client initialization with valid credentials."""
    with patch('os.path.exists', return_value=True):
        service = NLPService()
        client = service.get_client()
        assert client is not None
        assert isinstance(service.retry_config, retry.Retry)
        assert client._transport._host == 'us-central1-language.googleapis.com'

def test_client_initialization_failure(mock_settings):
    """Test client initialization with invalid credentials."""
    error_msg = "Google Cloud credentials file not found"
    
    with patch.object(NLPService, '_create_client', side_effect=ValueError(error_msg)):
        service = NLPService()
        with pytest.raises(ValueError, match=error_msg):
            service.get_client()

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=True)
def test_analyze_text_success(mock_acquire, nlp_service, mock_language_client):
    """Test successful text analysis."""
    # Mock successful API responses
    mock_entity = MagicMock()
    mock_entity.name = "Google"
    mock_entity.type_ = Entity.Type.ORGANIZATION
    mock_entity.salience = 0.8
    mock_entity.mentions = [MagicMock()]
    mock_entity.metadata = {}
    mock_entity.sentiment = MagicMock(score=0.6, magnitude=0.8)
    
    mock_entity_response = MagicMock(
        entities=[mock_entity],
        language="en"
    )
    
    mock_sentence = MagicMock()
    mock_sentence.text = MagicMock(content="Test text", begin_offset=0)
    mock_sentence.sentiment = MagicMock(score=0.5, magnitude=0.8)
    
    mock_sentiment_response = MagicMock(
        document_sentiment=MagicMock(score=0.5, magnitude=0.8),
        sentences=[mock_sentence],
        language="en"
    )
    
    mock_language_client.analyze_entities.return_value = mock_entity_response
    mock_language_client.analyze_sentiment.return_value = mock_sentiment_response
    
    nlp_service._client = mock_language_client
    
    result = nlp_service.analyze_text("Test text", language="en")
    
    assert 'entities' in result
    assert 'sentiment' in result
    assert 'language' in result
    assert result['language'] == "en"
    assert result['sentiment']['score'] == 0.5
    assert result['sentiment']['magnitude'] == 0.8
    assert len(result['entities']) == 1
    assert result['entities'][0]['name'] == "Google"
    assert result['entities'][0]['sentiment']['score'] == 0.6
    assert 'analyzed_at' in result

    # Verify request format
    entity_request = mock_language_client.analyze_entities.call_args[1]['request']
    assert entity_request['document']['content'] == "Test text"
    assert entity_request['document']['type'] == Document.Type.PLAIN_TEXT
    assert entity_request['document']['language_code'] == "en"
    assert entity_request['encoding_type'] == EncodingType.UTF8
    assert 'retry' in entity_request
    assert 'timeout' in entity_request

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=True)
def test_analyze_text_retry_on_timeout(mock_acquire, nlp_service, mock_language_client):
    """Test retry behavior on timeout."""
    # Mock timeout then success
    mock_entity_response = MagicMock(
        entities=[],
        language="en"
    )
    mock_sentiment_response = MagicMock(
        document_sentiment=MagicMock(score=0.5, magnitude=0.8),
        sentences=[],
        language="en"
    )

    # Set up the mock to fail first, then succeed
    mock_language_client.analyze_entities.side_effect = [
        exceptions.DeadlineExceeded("Timeout"),
        mock_entity_response
    ]
    mock_language_client.analyze_sentiment.return_value = mock_sentiment_response
    
    nlp_service._client = mock_language_client
    
    with patch('time.sleep'):  # Don't actually sleep in tests
        result = nlp_service.analyze_text("Test text")
        
        # Verify retry behavior
        assert mock_language_client.analyze_entities.call_count == 2
        calls = mock_language_client.analyze_entities.call_args_list
        for call_args in calls:
            request = call_args.kwargs['request']
            assert request['document']['content'] == "Test text"
            assert request['document']['type'] == Document.Type.PLAIN_TEXT
            assert request['encoding_type'] == EncodingType.UTF8
            assert 'retry' in request
            assert 'timeout' in request
        assert 'entities' in result

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=True)
def test_analyze_text_circuit_breaker(mock_acquire, nlp_service, mock_language_client):
    """Test circuit breaker behavior."""
    # Mock service unavailable
    mock_language_client.analyze_entities.side_effect = exceptions.ServiceUnavailable("Service down")
    mock_language_client.analyze_sentiment.side_effect = exceptions.ServiceUnavailable("Service down")
    
    nlp_service._client = mock_language_client
    
    with patch('time.sleep'), \
         patch('app.utils.decorators.CircuitBreaker.record_failure') as mock_record_failure:
        # Should fail after max retries and open circuit breaker
        with pytest.raises(exceptions.ServiceUnavailable):
            nlp_service.analyze_text("Test text")
            
        # Verify circuit breaker was opened
        assert mock_record_failure.called
        
        # Next call should fail fast with circuit breaker open
        with pytest.raises(exceptions.ServiceUnavailable):
            nlp_service.analyze_text("Test text")

@patch('app.services.nlp_service.RateLimiter.acquire', return_value=False)
def test_rate_limit_exceeded(mock_acquire, nlp_service):
    """Test rate limit exceeded behavior."""
    with pytest.raises(exceptions.ResourceExhausted, match="NLP API rate limit exceeded"):
        nlp_service.analyze_text("Test text")

def test_cleanup(nlp_service, mock_language_client):
    """Test cleanup behavior."""
    nlp_service._client = mock_language_client
    
    nlp_service.cleanup()
    
    mock_language_client.transport.close.assert_called_once()
    assert nlp_service._client is None 