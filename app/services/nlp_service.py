"""
Natural Language Processing service using Google Cloud Natural Language API.
"""

from typing import Optional, Dict, List, Any
import threading
from datetime import datetime
import time
import logging
from functools import lru_cache, wraps
from google.cloud import language_v2
from google.cloud.language_v2 import LanguageServiceClient
from google.api_core import retry, exceptions

from app.config.google_cloud import (
    get_credentials_path,
    NLP_API_TIMEOUT,
    NLP_REQUESTS_PER_MINUTE,
    NLP_BURST_SIZE,
    NLP_CACHE_TTL,
    NLP_CACHE_MAX_SIZE,
    NLP_MAX_RETRIES,
    NLP_INITIAL_RETRY_DELAY,
    NLP_MAX_RETRY_DELAY
)
from app.monitoring.performance import PerformanceMonitor

logger = logging.getLogger(__name__)

class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, rate: int, burst_size: int):
        self.rate = rate  # tokens per minute
        self.burst_size = burst_size
        self.tokens = burst_size
        self.last_update = time.time()
        self._lock = threading.Lock()
        
    def acquire(self) -> bool:
        """Acquire a token if available"""
        with self._lock:
            now = time.time()
            # Add new tokens based on time passed
            time_passed = now - self.last_update
            new_tokens = time_passed * (self.rate / 60.0)  # Convert rate to per-second
            self.tokens = min(self.burst_size, self.tokens + new_tokens)
            self.last_update = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

class NLPService:
    """Singleton service for Natural Language Processing"""
    
    _instance: Optional['NLPService'] = None
    _lock = threading.Lock()
    _client: Optional[LanguageServiceClient] = None
    
    def __new__(cls) -> 'NLPService':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """Initialize the service"""
        self.performance_monitor = PerformanceMonitor()
        self.rate_limiter = RateLimiter(NLP_REQUESTS_PER_MINUTE, NLP_BURST_SIZE)
        
    def get_client(self) -> LanguageServiceClient:
        """Get or create the API client"""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    credentials_path = get_credentials_path()
                    if not credentials_path:
                        raise ValueError("Invalid or missing GCP credentials")
                        
                    self._client = LanguageServiceClient.from_service_account_file(
                        credentials_path
                    )
        return self._client
    
    def _rate_limit_decorator(func):
        """Decorator to apply rate limiting"""
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.rate_limiter.acquire():
                raise Exception("Rate limit exceeded")
            return func(self, *args, **kwargs)
        return wrapper
    
    @_rate_limit_decorator
    @lru_cache(maxsize=NLP_CACHE_MAX_SIZE)
    def analyze_text(self, text: str) -> Dict[str, Any]:
        """
        Analyzes text using Google Cloud Natural Language API
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary containing analysis results
        """
        client = self.get_client()
        
        try:
            # Create document object
            document = {
                "content": text,
                "type_": language_v2.Document.Type.PLAIN_TEXT
            }
            
            # Configure retry behavior
            retry_config = retry.Retry(
                initial=NLP_INITIAL_RETRY_DELAY,
                maximum=NLP_MAX_RETRY_DELAY,
                multiplier=2.0,
                predicate=retry.if_exception_type(
                    ConnectionError,
                    TimeoutError,
                    exceptions.DeadlineExceeded
                )
            )
            
            with self.performance_monitor.monitor_api_call('analyze_entities'):
                entity_response = client.analyze_entities(
                    request={
                        "document": document,
                        "encoding_type": language_v2.EncodingType.UTF8
                    },
                    retry=retry_config,
                    timeout=NLP_API_TIMEOUT
                )
            
            with self.performance_monitor.monitor_api_call('analyze_sentiment'):
                sentiment_response = client.analyze_sentiment(
                    request={
                        "document": document,
                        "encoding_type": language_v2.EncodingType.UTF8
                    },
                    retry=retry_config,
                    timeout=NLP_API_TIMEOUT
                )
            
            return {
                'entities': self._format_entities(entity_response),
                'sentiment': self._format_sentiment(sentiment_response),
                'language': entity_response.language_code,
                'analyzed_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.performance_monitor.record_error('nlp_analysis', str(e))
            raise
    
    def _format_entities(self, response) -> List[Dict[str, Any]]:
        """Formats entity analysis response"""
        return [{
            'name': entity.name,
            'type': entity.type_.name,
            'metadata': dict(entity.metadata),
            'mentions': [{
                'text': mention.text.content,
                'type': mention.type_.name,
                'probability': mention.probability,
                'begin_offset': mention.text.begin_offset if mention.text.begin_offset else 0
            } for mention in entity.mentions]
        } for entity in response.entities]
    
    def _format_sentiment(self, response) -> Dict[str, Any]:
        """Formats sentiment analysis response"""
        return {
            'document_sentiment': {
                'score': response.document_sentiment.score,
                'magnitude': response.document_sentiment.magnitude
            },
            'sentences': [{
                'text': sentence.text.content,
                'sentiment': {
                    'score': sentence.sentiment.score,
                    'magnitude': sentence.sentiment.magnitude
                }
            } for sentence in response.sentences]
        }
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        with self._lock:
            if self._client is not None:
                self._client.transport.close()
                self._client = None 