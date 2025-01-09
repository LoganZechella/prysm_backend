import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from scrapfly import ScrapeConfig, ScrapflyClient

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
        logger.setLevel(logging.DEBUG)  # Set log level to debug for development
        
    async def _make_scrapfly_request(
        self, 
        url: str, 
        render_js: bool = False,
        country: str = "US",
        proxy_pool: str = "public",
        retry_attempts: int = 3,
        wait_for_selector: Optional[str] = None,
        rendering_wait: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        asp: bool = True,
        cache: bool = True,
        cache_ttl: int = 3600,  # 1 hour cache by default
        debug: bool = True
    ) -> Optional[BeautifulSoup]:
        """Common method for making Scrapfly requests with retries and error handling"""
        for attempt in range(retry_attempts):
            try:
                logger.debug(f"Making request to {url} (attempt {attempt + 1}/{retry_attempts})")
                config = ScrapeConfig(
                    url=url,
                    # Anti-scraping protection bypass (recommended)
                    asp=asp,
                    # Proxy settings
                    country=country,
                    proxy_pool=proxy_pool,
                    # JavaScript rendering settings
                    render_js=render_js,
                    wait_for_selector=wait_for_selector,
                    rendering_wait=rendering_wait,
                    # Cache settings (recommended when developing)
                    cache=cache,
                    cache_ttl=cache_ttl,
                    # Debug mode for more details in dashboard
                    debug=debug,
                    # Request customization
                    headers=headers,
                    cookies=cookies
                )
                
                result = await self.client.async_scrape(config)
                
                if not result.success:
                    logger.error(f"Failed to scrape {self.platform} (attempt {attempt + 1}/{retry_attempts}): {result.error}")
                    if attempt == retry_attempts - 1:
                        return None
                    continue
                    
                logger.debug(f"Successfully scraped {url}")
                return BeautifulSoup(result.content, 'html.parser')
                
            except Exception as e:
                logger.error(f"Error making Scrapfly request for {self.platform} (attempt {attempt + 1}/{retry_attempts}): {str(e)}")
                if attempt == retry_attempts - 1:
                    return None
                await asyncio.sleep(1)  # Wait before retrying
                continue
        
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