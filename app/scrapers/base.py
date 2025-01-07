import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json
from scrapfly import ScrapeConfig, ScrapflyClient

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Base class for event scrapers"""
    
    def __init__(self, platform: str):
        """Initialize the scraper"""
        self.platform = platform
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        self.session = None
        self.scrapfly_key = None
        self.retry_count = 3
        self.retry_delay = 5  # seconds
        
    def initialize_scrapfly(self, api_key: str):
        """Initialize Scrapfly client"""
        self.scrapfly_key = api_key
        self.client = ScrapflyClient(key=api_key)
    
    async def __aenter__(self):
        """Enter the context manager"""
        try:
            if not self.session:
                self.session = aiohttp.ClientSession(headers=self.headers)
            if not self.client and self.scrapfly_key:
                self.initialize_scrapfly(self.scrapfly_key)
            if not self.client:
                raise RuntimeError("Scrapfly client not initialized. Make sure scrapfly_key is set.")
            return self
        except Exception as e:
            logger.error(f"Error initializing scraper: {str(e)}")
            if self.session:
                await self.session.close()
                self.session = None
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
        except Exception as e:
            logger.error(f"Error cleaning up scraper: {str(e)}")
    
    @abstractmethod
    async def scrape_events(self, location: Dict[str, Any], date_range: Dict[str, datetime]) -> List[Dict[str, Any]]:
        """Scrape events for a given location and date range"""
        pass
    
    @abstractmethod
    async def get_event_details(self, event_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific event"""
        pass
    
    async def fetch_with_retry(self, config: ScrapeConfig) -> Optional[Any]:
        """Make a request with retries and proper error handling"""
        retries = 0
        while retries < self.retry_count:
            try:
                # Add headers to config
                config.headers = {**config.headers, **self.headers} if config.headers else self.headers
                
                # Use residential proxy pool for better success rate
                config.proxy_pool = "public_residential_pool"
                
                # Make request
                result = await self.client.async_scrape(config)
                
                if result and hasattr(result, 'content'):
                    return result.content
                
                # Handle specific error cases
                if "ERR::ASP::SHIELD_PROTECTION_FAILED" in str(result.error):
                    logger.warning(f"ASP shield failed, retrying in {self.retry_delay * 2} seconds...")
                    await asyncio.sleep(self.retry_delay * 2)
                    retries += 1
                    continue
                
                if "ERR::SCRAPE::UPSTREAM_WEBSITE_ERROR" in str(result.error):
                    logger.warning(f"Connection refused, retrying in {self.retry_delay * 3} seconds...")
                    await asyncio.sleep(self.retry_delay * 3)
                    retries += 1
                    continue
                
                if "ERR::SCRAPE::BAD_UPSTREAM_RESPONSE" in str(result.error):
                    logger.error(f"Bad upstream response: {result.error}")
                    return None
                
                logger.error(f"Request failed: {result.error}")
                return None
                
            except Exception as e:
                logger.error(f"Error making request: {str(e)}")
                retries += 1
                if retries < self.retry_count:
                    await asyncio.sleep(self.retry_delay * (retries + 1))
                    continue
                return None
        
        logger.error(f"Failed after {self.retry_count} retries")
        return None
    
    def parse_html(self, html: str) -> BeautifulSoup:
        """Parse HTML content"""
        return BeautifulSoup(html, 'html.parser')
    
    def clean_text(self, text: Optional[str]) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        return " ".join(text.split())
    
    def extract_date(self, date_str: str) -> Optional[datetime]:
        """Extract datetime from string"""
        try:
            # Add specific date parsing logic based on source format
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except Exception as e:
            logger.error(f"Error parsing date {date_str}: {str(e)}")
            return None
    
    def standardize_event(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize event data to match our schema"""
        try:
            return {
                "event_id": raw_event.get("id", ""),
                "title": raw_event.get("title", ""),
                "description": raw_event.get("description", ""),
                "start_datetime": raw_event.get("start_datetime"),
                "end_datetime": raw_event.get("end_datetime"),
                "location": {
                    "venue_name": raw_event.get("venue", {}).get("name", ""),
                    "address": raw_event.get("venue", {}).get("address", ""),
                    "city": raw_event.get("venue", {}).get("city", ""),
                    "state": raw_event.get("venue", {}).get("state", ""),
                    "country": raw_event.get("venue", {}).get("country", ""),
                    "coordinates": raw_event.get("venue", {}).get("coordinates", {"lat": 0.0, "lng": 0.0})
                },
                "categories": raw_event.get("categories", []),
                "tags": raw_event.get("tags", []),
                "price_info": {
                    "currency": raw_event.get("price", {}).get("currency", "USD"),
                    "min_price": raw_event.get("price", {}).get("min", 0.0),
                    "max_price": raw_event.get("price", {}).get("max", 0.0),
                    "price_tier": raw_event.get("price", {}).get("tier", "free")
                },
                "images": raw_event.get("images", []),
                "source": {
                    "platform": self.platform,
                    "url": raw_event.get("url", ""),
                    "last_updated": datetime.utcnow().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error standardizing event: {str(e)}")
            return raw_event 