import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from scrapfly import ScrapeConfig, ScrapflyClient
from app.utils.rate_limiter import RateLimiter

# Set up logging
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Base scraper class that all scrapers must inherit from"""
    def __init__(self, platform: str):
        self.platform = platform

class ScrapflyBaseScraper(BaseScraper):
    """Enhanced base scraper for Scrapfly-based scrapers"""
    
    def __init__(self, platform: str, scrapfly_key: str):
        super().__init__(platform)
        self.client = ScrapflyClient(key=scrapfly_key)
        
        # Default configuration based on Scrapfly best practices
        self.config = {
            'render_js': True,  # Enable JS rendering by default
            'proxy_pool': "residential",  # Better success rate with residential IPs
            'retry_attempts': 4,
            'wait_for_selector': 'body',  # Wait for page load
            'rendering_wait': 3000,  # 3s wait for dynamic content
            'cache': True,
            'cache_ttl': 1800,  # 30 min cache
            'country': 'US',
            'asp': True,  # Enable Anti-Scraping Protection
            'timeout': 60000,  # 60s timeout for ASP
        }
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(rate_limit=10)

    async def _make_request_with_retry(self, url: str, config: Dict[str, Any]) -> Optional[BeautifulSoup]:
        """Make request with improved error handling based on Scrapfly docs"""
        for attempt in range(config['retry_attempts']):
            try:
                await self.rate_limiter.acquire()
                
                result = await self.client.async_scrape(ScrapeConfig(
                    url=url,
                    **{k: v for k, v in config.items() 
                       if k in ScrapeConfig.__annotations__}
                ))

                if not result.success:
                    error = result.error
                    # Check if error is retryable based on Scrapfly error codes
                    if error.get('retryable', False) and attempt < config['retry_attempts'] - 1:
                        wait_time = (2 ** attempt) * 1  # Exponential backoff
                        logger.info(f"Retryable error, waiting {wait_time}s: {error.get('description')}")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    logger.error(f"Scrapfly error: {error.get('description')} (Code: {error.get('code')})")
                    return None

                return BeautifulSoup(result.content, 'html.parser')

            except Exception as e:
                logger.error(f"Request error: {str(e)}")
                if attempt < config['retry_attempts'] - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return None
            
    async def _make_scrapfly_request(self, url: str, **kwargs) -> Optional[BeautifulSoup]:
        """Make a request using Scrapfly with retries and error handling"""
        try:
            await self.rate_limiter.acquire()
            
            config = {**self.config, **kwargs}
            result = await self.client.async_scrape(ScrapeConfig(
                url=url,
                **{k: v for k, v in config.items() 
                   if k in ScrapeConfig.__annotations__}
            ))

            if not result.success:
                error = result.error
                logger.error(f"Scrapfly error: {error.get('description')} (Code: {error.get('code')})")
                return None

            return BeautifulSoup(result.content, 'html.parser')

        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None

    @abstractmethod
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Must be implemented by each specific scraper
        
        Args:
            location: Dictionary containing location details (city, state, country, etc.)
            date_range: Dictionary with start and end datetime objects
            categories: Optional list of event categories to filter by
            
        Returns:
            List of event dictionaries with standardized fields
        """
        pass 

    async def _handle_scrapfly_error(self, error: Dict[str, Any]) -> bool:
        """Handle specific Scrapfly error codes
        Returns True if the error is retryable
        """
        error_code = error.get('code', '')
        
        if error_code == 'ERR::ASP::SHIELD_ERROR':
            logger.error("ASP Shield error - will not retry")
            return False
            
        if error_code == 'ERR::SCRAPE::OPERATION_TIMEOUT':
            logger.warning("Timeout error - increasing timeout for next attempt")
            self.config['timeout'] = min(90000, self.config['timeout'] * 1.5)
            return True
            
        if error_code == 'ERR::THROTTLE::MAX_REQUEST_RATE_EXCEEDED':
            logger.warning("Rate limit exceeded - adding delay")
            await asyncio.sleep(5)
            return True
            
        # Default to retryable for unknown errors
        return True 