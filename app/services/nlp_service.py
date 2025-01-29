"""
Natural Language Processing service using Google Cloud Natural Language API.
"""

from typing import Optional, Dict, List, Any, Sequence
import threading
from datetime import datetime
import time
import logging
from functools import lru_cache, wraps
from google.cloud import language_v2
from google.cloud.language_v2 import LanguageServiceClient, Document, EncodingType, Entity
from google.api_core import exceptions, retry, operation
from google.api_core.client_options import ClientOptions

from app.config.google_cloud import (
    get_language_client,
    get_nlp_config
)
from app.utils.decorators import circuit_breaker, with_retry

logger = logging.getLogger(__name__)

class RateLimiter:
    """Rate limiter for API calls"""
    
    def __init__(self, rate: int, burst_size: int):
        """
        Initialize rate limiter.
        
        Args:
            rate: Requests per minute
            burst_size: Maximum burst size
        """
        self.rate = rate / 60.0  # Convert to requests per second
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_update = time.time()
        self._lock = threading.Lock()
        
    def acquire(self) -> bool:
        """
        Attempt to acquire a token.
        
        Returns:
            bool: True if token acquired, False otherwise
        """
        with self._lock:
            now = time.time()
            time_passed = now - self.last_update
            self.tokens = min(
                self.burst_size,
                self.tokens + time_passed * self.rate
            )
            
            if self.tokens >= 1:
                self.tokens -= 1
                self.last_update = now
                return True
            return False

class NLPService:
    """Singleton service for Natural Language Processing"""
    
    _instance: Optional['NLPService'] = None
    _lock = threading.Lock()
    _client: Optional[LanguageServiceClient] = None
    
    def __new__(cls) -> 'NLPService':
        """Ensure singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._initialize()
                    cls._instance = instance
        return cls._instance
        
    def _initialize(self) -> None:
        """Initialize service components."""
        config = get_nlp_config()
        self.rate_limiter = RateLimiter(
            rate=config['requests_per_minute'],
            burst_size=config['burst_size']
        )
        self.timeout = config['timeout']
        self.retry_config = retry.Retry(
            initial=config['initial_retry_delay'],
            maximum=config['max_retry_delay'],
            multiplier=2.0,
            predicate=retry.if_exception_type(
                exceptions.DeadlineExceeded,
                exceptions.ServiceUnavailable,
                exceptions.ResourceExhausted,
                ConnectionError,
                TimeoutError
            )
        )
        
    def _create_client(self) -> LanguageServiceClient:
        """Create a new NLP client."""
        try:
            config = get_nlp_config()
            client = get_language_client()
            client._transport._host = f"{config['api_region']}-language.googleapis.com"
            return client
        except Exception as e:
            logger.error(f"Failed to create NLP client: {str(e)}")
            raise
        
    @circuit_breaker("nlp_api")
    def get_client(self) -> LanguageServiceClient:
        """Get or create NLP client."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    self._client = self._create_client()
        return self._client
        
    def _rate_limit_decorator(func):
        """Decorator to apply rate limiting."""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.rate_limiter.acquire():
                raise exceptions.ResourceExhausted(
                    "NLP API rate limit exceeded"
                )
            return func(self, *args, **kwargs)
        return wrapper
        
    @_rate_limit_decorator
    @lru_cache(maxsize=1000)  # Use config value
    @with_retry(
        max_retries=3,
        initial_delay=1.0,
        exponential_base=2.0,
        exceptions=(
            ConnectionError,
            TimeoutError,
            exceptions.DeadlineExceeded,
            exceptions.ServiceUnavailable,
            exceptions.ResourceExhausted
        ),
        circuit_breaker_name="nlp_api"
    )
    def analyze_text(self, text: str, language: str = None) -> Dict[str, Any]:
        """
        Analyze text using Google Cloud Natural Language API.
        
        Args:
            text: Text to analyze
            language: Optional ISO-639-1 language code
            
        Returns:
            Dictionary containing analysis results
            
        Raises:
            ValueError: If text is empty
            google.api_core.exceptions.*: For API errors
        """
        if not text.strip():
            raise ValueError("Text cannot be empty")
            
        client = self.get_client()
        
        # Prepare document
        document = {
            'content': text,
            'type': Document.Type.PLAIN_TEXT,
        }
        if language:
            document['language_code'] = language
            
        try:
            # Get entity analysis
            entity_response = client.analyze_entities(
                request={
                    "document": document,
                    "encoding_type": EncodingType.UTF8,
                    "retry": self.retry_config,
                    "timeout": self.timeout
                }
            )
            
            # Get sentiment analysis
            sentiment_response = client.analyze_sentiment(
                request={
                    "document": document,
                    "encoding_type": EncodingType.UTF8,
                    "retry": self.retry_config,
                    "timeout": self.timeout
                }
            )
            
            return {
                'entities': self._format_entities(entity_response.entities),
                'sentiment': self._format_sentiment(sentiment_response),
                'language': entity_response.language,
                'analyzed_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"NLP analysis failed: {str(e)}")
            raise
            
    def _format_entities(self, entities: Sequence[Entity]) -> List[Dict[str, Any]]:
        """Format entity analysis response."""
        formatted_entities = []
        for entity in entities:
            formatted_entity = {
                'name': entity.name,
                'type': Entity.Type(entity.type_).name,
                'salience': float(entity.salience),
                'mentions': len(entity.mentions),
                'metadata': dict(entity.metadata)
            }
            
            # Add optional fields if present
            if hasattr(entity, 'sentiment') and entity.sentiment:
                formatted_entity['sentiment'] = {
                    'score': float(entity.sentiment.score),
                    'magnitude': float(entity.sentiment.magnitude)
                }
                
            formatted_entities.append(formatted_entity)
            
        return formatted_entities
        
    def _format_sentiment(self, response) -> Dict[str, Any]:
        """Format sentiment analysis response."""
        return {
            'score': float(response.document_sentiment.score),
            'magnitude': float(response.document_sentiment.magnitude),
            'language': response.language,
            'sentences': [{
                'text': sentence.text.content,
                'score': float(sentence.sentiment.score),
                'magnitude': float(sentence.sentiment.magnitude),
                'begin_offset': sentence.text.begin_offset
            } for sentence in response.sentences]
        }
        
    def cleanup(self) -> None:
        """Cleanup resources."""
        if self._client:
            self._client.transport.close()
            self._client = None 