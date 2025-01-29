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
from abc import ABC, abstractmethod
from app.monitoring.performance import PerformanceMonitor
from app.schemas.validation import ValidationPipeline, SchemaVersion

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

class BaseScraper(ABC):
    """Base class for all event scrapers."""
    
    def __init__(self):
        self.performance_monitor = PerformanceMonitor()
        self.validation_pipeline = ValidationPipeline()
        
    @abstractmethod
    async def search_events(
        self,
        location: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        categories: Optional[List[str]] = None,
        keywords: Optional[List[str]] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Search for events based on criteria.
        
        Args:
            location: Location to search in
            start_date: Optional start date for events
            end_date: Optional end date for events
            categories: Optional list of event categories
            keywords: Optional list of keywords to search for
            **kwargs: Additional search parameters
            
        Returns:
            List of event dictionaries
        """
        pass
        
    @abstractmethod
    async def get_event_details(self, event_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific event.
        
        Args:
            event_id: ID of the event to fetch
            
        Returns:
            Event details dictionary
        """
        pass
        
    def validate_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate event data against schema.
        
        Args:
            event_data: Raw event data
            
        Returns:
            Validated event data
        """
        try:
            return self.validation_pipeline.validate_event(
                event_data,
                target_version=SchemaVersion.V2
            )
        except Exception as e:
            logger.error(
                f"Event validation failed: {str(e)}",
                extra={
                    'event_id': event_data.get('external_id'),
                    'source': event_data.get('source')
                }
            )
            raise
            
    def normalize_location(self, location_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize location data to standard format.
        
        Args:
            location_data: Raw location data
            
        Returns:
            Normalized location dictionary
        """
        required_fields = {'name', 'city', 'country'}
        missing_fields = required_fields - set(location_data.keys())
        if missing_fields:
            raise ValueError(f"Missing required location fields: {missing_fields}")
            
        return {
            'name': location_data['name'].strip(),
            'address': location_data.get('address', '').strip(),
            'city': location_data['city'].strip(),
            'state': location_data.get('state', '').strip(),
            'country': location_data['country'].strip(),
            'postal_code': location_data.get('postal_code', '').strip(),
            'latitude': float(location_data['latitude']) if 'latitude' in location_data else None,
            'longitude': float(location_data['longitude']) if 'longitude' in location_data else None
        }
        
    def normalize_price(self, price_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize price data to standard format.
        
        Args:
            price_data: Raw price data
            
        Returns:
            Normalized price dictionary
        """
        required_fields = {'amount', 'currency'}
        missing_fields = required_fields - set(price_data.keys())
        if missing_fields:
            raise ValueError(f"Missing required price fields: {missing_fields}")
            
        return {
            'amount': float(price_data['amount']),
            'currency': price_data['currency'].strip().upper(),
            'tier': price_data.get('tier', '').strip()
        }
        
    def normalize_categories(self, categories: List[str]) -> List[str]:
        """
        Normalize category names.
        
        Args:
            categories: List of category names
            
        Returns:
            List of normalized category names
        """
        return [cat.strip().lower() for cat in categories if cat.strip()]
        
    def record_error(self, operation: str, error: Exception):
        """
        Record scraping error.
        
        Args:
            operation: Name of the operation that failed
            error: The exception that occurred
        """
        self.performance_monitor.record_error(
            operation=operation,
            error_message=str(error)
        )
        logger.error(
            f"Scraping error in {operation}: {str(error)}",
            exc_info=error
        ) 