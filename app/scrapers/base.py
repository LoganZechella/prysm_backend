"""
Base scraper class for all Scrapfly-based scrapers.

This module provides the base scraper implementation with common functionality:
- Rate limiting
- Error handling and retries
- Request configuration
- Response processing
- Health monitoring
- Circuit breaker protection
"""

import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup

from app.utils.rate_limiter import RateLimiter
from app.utils.retry_handler import RetryHandler
from app.utils.monitoring import ScraperMonitor
from app.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError
from app.utils.scraper_config import ScraperConfig

logger = logging.getLogger(__name__)

class ScrapflyError(Exception):
    """Base exception for Scrapfly-related errors"""
    pass

class ScrapflyRateLimitError(ScrapflyError):
    """Raised when Scrapfly rate limit is exceeded"""
    pass

class ScrapflyResponseError(ScrapflyError):
    """Raised when Scrapfly returns an error response"""
    pass

class ScrapflyBaseScraper:
    """Base class for Scrapfly-based scrapers"""
    
    def __init__(
        self,
        api_key: str,
        requests_per_second: float = 2.0,
        max_retries: int = 3,
        error_threshold: float = 0.3,
        monitor_window: int = 15,
        circuit_failure_threshold: int = 5,
        circuit_recovery_timeout: float = 60.0
    ):
        """
        Initialize the base scraper.
        
        Args:
            api_key: Scrapfly API key
            requests_per_second: Maximum requests per second
            max_retries: Maximum number of retry attempts
            error_threshold: Maximum acceptable error rate
            monitor_window: Time window for monitoring in minutes
            circuit_failure_threshold: Failures before circuit opens
            circuit_recovery_timeout: Seconds before recovery attempt
        """
        self.api_key = api_key
        self.session = None
        self.platform = None  # Must be set by subclasses
        
        # Initialize utilities
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            burst_limit=int(requests_per_second * 2),
            max_delay=60.0
        )
        
        self.retry_handler = RetryHandler(
            max_retries=max_retries,
            base_delay=1.0,
            max_delay=30.0,
            jitter=True
        )
        
        self.monitor = ScraperMonitor(
            error_threshold=error_threshold,
            window_minutes=monitor_window
        )
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_failure_threshold,
            recovery_timeout=circuit_recovery_timeout
        )
        
    async def _init_session(self) -> None:
        """Initialize aiohttp session if not already initialized"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
            
    async def _close_session(self) -> None:
        """Close aiohttp session if open"""
        if self.session is not None:
            await self.session.close()
            self.session = None
            
    def _should_retry(self, error: Exception) -> bool:
        """
        Determine if an error should trigger a retry attempt.
        
        Args:
            error: The exception that occurred
            
        Returns:
            True if the error should trigger a retry, False otherwise
        """
        # Don't retry rate limit errors
        if isinstance(error, ScrapflyRateLimitError):
            return False
            
        # Retry network errors and 5xx responses
        if isinstance(error, (aiohttp.ClientError, ScrapflyResponseError)):
            return True
            
        return False
        
    def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get request configuration for the current platform.
        
        Args:
            url: Target URL to scrape
            config: Optional configuration overrides
            
        Returns:
            Complete request configuration
        """
        if not self.platform:
            raise ValueError("Platform must be set by subclass")
            
        # Get platform-specific configuration
        platform_config = ScraperConfig.get_config(self.platform)
        proxy_config = ScraperConfig.get_proxy_config(self.platform)
        
        # Merge configurations in order of precedence
        request_config = {
            **platform_config,
            **proxy_config,
            'api_key': self.api_key,
            'url': url
        }
        
        # Apply any custom overrides
        if config:
            request_config.update(config)
            
        return request_config
        
    async def _make_request(
        self,
        url: str,
        config: Optional[Dict[str, Any]] = None
    ) -> BeautifulSoup:
        """
        Make a request to Scrapfly API with rate limiting and retries.
        
        Args:
            url: Target URL to scrape
            config: Optional Scrapfly configuration overrides
            
        Returns:
            BeautifulSoup object of the parsed response
            
        Raises:
            ScrapflyError: If the request fails after all retries
        """
        start_time = time.monotonic()
        request_context = {'url': url}
        
        try:
            async def execute_request():
                await self._init_session()
                request_config = self._get_request_config(url, config)
                
                # Acquire rate limit token
                async with self.rate_limiter:
                    async with self.session.get(
                        'https://api.scrapfly.io/scrape',
                        params=request_config
                    ) as response:
                        # Handle error responses
                        if response.status == 429:
                            raise ScrapflyRateLimitError("Scrapfly rate limit exceeded")
                        elif response.status >= 400:
                            raise ScrapflyResponseError(
                                f"Scrapfly API error: {response.status} - {await response.text()}"
                            )
                            
                        # Parse successful response
                        data = await response.json()
                        if not data.get('result', {}).get('content'):
                            raise ScrapflyResponseError("Empty response from Scrapfly")
                            
                        return BeautifulSoup(
                            data['result']['content'],
                            'html.parser'
                        )
                        
            # Execute with circuit breaker protection
            result = await self.circuit_breaker.execute(
                lambda: self.retry_handler.execute_with_retry(
                    execute_request,
                    should_retry=self._should_retry
                )
            )
            
            # Record successful request
            self.monitor.record_request(
                self.platform,
                success=True,
                response_time=time.monotonic() - start_time,
                context=request_context
            )
            
            return result
            
        except Exception as e:
            # Record failed request
            self.monitor.record_request(
                self.platform,
                success=False,
                response_time=time.monotonic() - start_time,
                context=request_context
            )
            
            # Record error
            self.monitor.record_error(
                self.platform,
                error=e,
                context=request_context
            )
            
            logger.error(
                f"Failed to scrape {url}",
                extra={
                    'scraper': self.platform,
                    'error': str(e),
                    'url': url
                }
            )
            raise
            
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from the target platform.
        
        Args:
            location: Location parameters (city, state, lat, lng)
            date_range: Date range to search
            categories: Optional list of event categories
            
        Returns:
            List of scraped events
            
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement scrape_events")
        
    def get_health_metrics(self) -> Dict[str, Any]:
        """Get current health and performance metrics"""
        return {
            'monitor': self.monitor.get_metrics(self.platform),
            'circuit_breaker': self.circuit_breaker.get_metrics(),
            'rate_limiter': self.rate_limiter.get_stats()
        }
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self._init_session()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self._close_session() 