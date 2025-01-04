import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import json

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Base class for event scrapers"""
    
    def __init__(self, platform: str):
        """Initialize the scraper"""
        self.platform = platform
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = None
    
    async def __aenter__(self):
        """Create aiohttp session"""
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
    
    @abstractmethod
    async def scrape_events(self, location: Dict[str, Any], date_range: Optional[Dict[str, datetime]] = None) -> List[Dict[str, Any]]:
        """Scrape events for a given location and date range"""
        pass
    
    @abstractmethod
    async def get_event_details(self, event_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific event"""
        pass
    
    async def fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with rate limiting and retries"""
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            for attempt in range(3):  # 3 retries
                try:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:  # Too Many Requests
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited. Waiting {wait_time} seconds.")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"Error fetching {url}: Status {response.status}")
                            return None
                except aiohttp.ClientError as e:
                    logger.error(f"Error fetching {url}: {str(e)}")
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            return None
            
        except Exception as e:
            logger.error(f"Error in fetch_page: {str(e)}")
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