"""
Eventbrite scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import json
from bs4 import BeautifulSoup

from app.scrapers.base import ScrapflyBaseScraper

logger = logging.getLogger(__name__)

class EventbriteScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Eventbrite events using Scrapfly"""
    
    def __init__(self, api_key: str):
        """Initialize the Eventbrite scraper"""
        super().__init__(api_key)
        self.platform = "eventbrite"
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from Eventbrite.
        
        Args:
            location: Location parameters (city, state, lat, lng)
            date_range: Date range to search
            categories: Optional list of event categories
            
        Returns:
            List of scraped events
        """
        events = []
        page = 1
        
        while True:
            url = self._build_search_url(location, date_range, categories, page)
            try:
                soup = await self._make_request(url)
                if not soup:
                    break
                    
                new_events = self._parse_json_ld(soup)
                if not new_events:
                    break
                    
                events.extend(new_events)
                page += 1
                
            except Exception as e:
                logger.error(f"Error scraping Eventbrite page {page}: {str(e)}")
                break
                
        return events
        
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> str:
        """Build the Eventbrite search URL with parameters"""
        base_url = "https://www.eventbrite.com/d"
        
        # Format location
        location_str = f"{location['city']}/{location['state']}"
        
        # Format dates
        start_date = date_range['start'].strftime("%Y-%m-%d")
        end_date = date_range['end'].strftime("%Y-%m-%d")
        
        # Build query parameters
        params = [
            f"--location-address={location_str}",
            f"--date={start_date}--{end_date}",
            f"--page={page}"
        ]
        
        if categories:
            params.append(f"--category={','.join(categories)}")
            
        return f"{base_url}{'/' if params else ''}{'/'.join(params)}"
        
    def _parse_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse events from JSON-LD data in the page"""
        events = []
        
        # Find the JSON-LD script tag containing event data
        script_tag = soup.find("script", {"type": "application/ld+json"})
        if not script_tag:
            return events
            
        try:
            # Parse JSON-LD data
            data = json.loads(script_tag.string)
            if not isinstance(data, dict) or "itemListElement" not in data:
                return events
                
            # Extract events from itemListElement
            for item in data["itemListElement"]:
                try:
                    event_data = item["item"]
                    
                    # Parse start and end times
                    start_time = datetime.fromisoformat(event_data["startDate"].replace("Z", "+00:00"))
                    end_time = datetime.fromisoformat(event_data["endDate"].replace("Z", "+00:00"))
                    
                    # Extract location data
                    location = event_data.get("location", {})
                    address = location.get("address", {})
                    geo = location.get("geo", {})
                    
                    venue_info = {
                        "venue_name": location.get("name", ""),
                        "address": ", ".join(filter(None, [
                            address.get("streetAddress", ""),
                            address.get("addressLocality", ""),
                            address.get("addressRegion", ""),
                            address.get("postalCode", "")
                        ])),
                        "latitude": float(geo.get("latitude", 0)),
                        "longitude": float(geo.get("longitude", 0))
                    }
                    
                    # Extract event URL and ID
                    url = event_data.get("url", "")
                    event_id = url.split("-")[-1] if url else ""
                    
                    event = {
                        "title": event_data.get("name", ""),
                        "description": event_data.get("description", ""),
                        "start_time": start_time,
                        "end_time": end_time,
                        "location": venue_info,
                        "image_url": event_data.get("image", ""),
                        "source": self.platform,
                        "event_id": event_id,
                        "url": url
                    }
                    
                    events.append(event)
                    
                except Exception as e:
                    logger.error(f"Error parsing event from JSON-LD: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing JSON-LD data: {str(e)}")
            
        return events 