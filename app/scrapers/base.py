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
from scrapfly import ScrapeConfig, ScrapflyClient

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
        **kwargs
    ):
        """Initialize the base scraper."""
        self.api_key = api_key
        self.platform = platform.lower()
        self.client = ScrapflyClient(api_key)
            
    async def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get base Scrapfly configuration"""
        base_config = {
            'url': url,
            'render_js': True,
            'asp': True,
            'country': 'us',
            'headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        }
        
        if config:
            base_config.update(config)
            
        return base_config
        
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