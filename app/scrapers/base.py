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
from typing import Dict, Any, Optional, List, Callable, Awaitable, TypeVar, Generic, Union
from datetime import datetime
import aiohttp
from bs4 import BeautifulSoup
import asyncio
import json
import re

from app.utils.rate_limiter import RateLimiter
from app.utils.retry_handler import RetryHandler, RetryError
from app.utils.monitoring import ScraperMonitor
from app.utils.circuit_breaker import CircuitBreaker, CircuitBreakerError
from app.utils.scraper_config import ScraperConfig

logger = logging.getLogger(__name__)

T = TypeVar('T')

class ScrapflyError(Exception):
    """Base exception for Scrapfly-related errors"""
    pass

class ScrapflyRateLimitError(ScrapflyError):
    """Raised when Scrapfly rate limit is exceeded"""
    pass

class ScrapflyResponseError(ScrapflyError):
    """Raised when Scrapfly returns an error response"""
    pass

class ScrapflyBaseScraper(Generic[T]):
    """Base class for Scrapfly-based scrapers"""
    
    def __init__(
        self,
        api_key: str,
        platform: str,
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
            platform: Platform name (eventbrite, meetup, facebook)
            requests_per_second: Maximum requests per second
            max_retries: Maximum number of retry attempts
            error_threshold: Maximum acceptable error rate
            monitor_window: Time window for monitoring in minutes
            circuit_failure_threshold: Failures before circuit opens
            circuit_recovery_timeout: Seconds before recovery attempt
        """
        self.api_key = api_key
        self.session = None
        self.platform = platform.lower()  # Store platform name in lowercase
        
        # Initialize Scrapfly client
        from scrapfly import ScrapeConfig, ScrapflyClient
        self.scrapfly = ScrapflyClient(key=api_key)
        
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
            jitter='true'
        )
        
        self.monitor = ScraperMonitor(
            error_threshold=error_threshold,
            window_minutes=monitor_window
        )
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_failure_threshold,
            recovery_timeout=circuit_recovery_timeout
        )
        
    async def reset(self) -> None:
        """Reset the scraper state to initial conditions"""
        # Reset circuit breaker state
        self.circuit_breaker.reset()
        
        # Clear monitoring history
        self.monitor.clear_history()
        
        # Reset rate limiter
        self.rate_limiter.reset()
        
        # Close and reinitialize session
        await self._close_session()
        await self._init_session()
        
        logger.info("Scraper reset to initial state")
            
    async def _init_session(self) -> None:
        """Initialize aiohttp session if not already initialized"""
        if self.session is None:
            self.session = aiohttp.ClientSession()
            
    async def _close_session(self) -> None:
        """Close aiohttp session if open"""
        if self.session is not None:
            await self.session.close()
            self.session = None
            
    def _should_retry(self, error: Exception) -> str:
        """
        Determine if an error should trigger a retry attempt.
        
        Args:
            error: The exception that occurred
            
        Returns:
            'true' if the error should trigger a retry, 'false' otherwise
        """
        # Don't retry rate limit errors
        if isinstance(error, ScrapflyRateLimitError):
            return 'false'
            
        # Don't retry circuit breaker errors
        if isinstance(error, CircuitBreakerError):
            return 'false'
            
        # Retry network errors and 5xx responses
        if isinstance(error, (aiohttp.ClientError, ScrapflyResponseError)):
            return 'true'
            
        return 'false'
        
    async def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get base Scrapfly configuration"""
        base_config = {
            'url': url,
            'render_js': True,  # Enable JavaScript rendering by default
            'asp': True,  # Enable anti-scraping protection
            'country': 'us',  # Set country for geo-targeting
            'headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        }
        
        # Merge with provided config if any
        if config:
            base_config.update(config)
            
        return base_config
        
    async def _make_request(
        self,
        url: str,
        config: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None
    ) -> Union[BeautifulSoup, str]:
        """Make a request using Scrapfly"""
        try:
            request_config = await self._get_request_config(url, config)
            
            # Create ScrapeConfig object with correct parameters
            from scrapfly import ScrapeConfig
            scrape_config = ScrapeConfig(
                url=url,
                render_js=request_config.get('render_js', True),
                asp=request_config.get('asp', True),
                headers=request_config.get('headers', {}),
                cookies={},  # Initialize empty cookies dict
                body=json_data if json_data else None,
                method=request_config.get('method', 'GET')
            )
            
            # Make request
            result = await self.scrapfly.async_scrape(scrape_config)
            
            if not result:
                logger.warning(f"[{self.platform}] Empty response from Scrapfly")
                return None
                
            logger.debug(f"[{self.platform}] Response status: {result.status_code}")
            logger.debug(f"[{self.platform}] Response headers: {dict(result.headers)}")
            
            content = result.content
            if not content:
                logger.warning(f"[{self.platform}] Empty content in response")
                return None
                
            logger.debug(f"[{self.platform}] Response content type: {type(content)}")
            logger.debug(f"[{self.platform}] First 2000 chars of response: {content[:2000]}")
            
            # For JSON requests, return raw content
            if json_data:
                return content
                
            # For HTML requests, parse with BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            # Check for error pages
            error_indicators = [
                "Access Denied",
                "Security Check",
                "Captcha",
                "Too Many Requests",
                "Rate Limit Exceeded"
            ]
            
            page_text = soup.get_text().lower()
            if any(indicator.lower() in page_text for indicator in error_indicators):
                logger.warning(f"[{self.platform}] Detected error page")
                raise ScrapflyError("Received error page instead of content")
                
            return soup
            
        except Exception as e:
            logger.error(f"[{self.platform}] Request failed: {str(e)}")
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