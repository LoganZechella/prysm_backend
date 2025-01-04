import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)

class TicketmasterScraper(BaseScraper):
    """Scraper for Ticketmaster events using their Discovery API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the scraper with API key"""
        super().__init__(platform="ticketmaster")
        self.api_key = api_key or os.getenv("TICKETMASTER_API_KEY")
        if not self.api_key:
            raise ValueError("Ticketmaster API key is required")
        self.base_url = "https://app.ticketmaster.com/discovery/v2"
        
    async def scrape_events(self, location: Dict[str, Any], date_range: Optional[Dict[str, datetime]] = None) -> List[Dict[str, Any]]:
        """Scrape events for a given location and date range"""
        events = []
        
        # Build query parameters
        params = {
            "apikey": self.api_key,
            "city": location.get("city"),
            "stateCode": location.get("state"),
            "countryCode": location.get("country", "US"),
            "size": 200  # Maximum allowed per request
        }
        
        if date_range:
            params["startDateTime"] = date_range["start"].strftime("%Y-%m-%dT%H:%M:%SZ")
            params["endDateTime"] = date_range["end"].strftime("%Y-%m-%dT%H:%M:%SZ")
        
        url = f"{self.base_url}/events.json"
        
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if "_embedded" in data and "events" in data["_embedded"]:
                        raw_events = data["_embedded"]["events"]
                        for raw_event in raw_events:
                            event = await self.get_event_details(raw_event["id"])
                            if event:
                                events.append(event)
                else:
                    logger.error(f"Failed to fetch events: {response.status}")
        
        except Exception as e:
            logger.error(f"Error scraping events: {str(e)}")
        
        return events
    
    async def get_event_details(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific event"""
        url = f"{self.base_url}/events/{event_id}"
        params = {"apikey": self.api_key}
        
        try:
            if not self.session:
                return None
                
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    raw_event = await response.json()
                    event = self.standardize_event(raw_event)
                    if event:
                        return event
                else:
                    logger.error(f"Failed to fetch event details: {response.status}")
        except Exception as e:
            logger.error(f"Error getting event details: {str(e)}")
        
        return None
    
    def standardize_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Ticketmaster event format to our standard schema"""
        try:
            # Extract venue information
            venue = raw_event.get("_embedded", {}).get("venues", [{}])[0]
            
            # Extract price information
            priceRanges = raw_event.get("priceRanges", [{}])[0]
            
            # Build standardized event
            return {
                "event_id": f"tm_{raw_event['id']}",
                "title": raw_event.get("name"),
                "description": raw_event.get("description", ""),
                "start_datetime": raw_event.get("dates", {}).get("start", {}).get("dateTime"),
                "end_datetime": raw_event.get("dates", {}).get("end", {}).get("dateTime"),
                "location": {
                    "venue_name": venue.get("name"),
                    "address": venue.get("address", {}).get("line1"),
                    "city": venue.get("city", {}).get("name"),
                    "state": venue.get("state", {}).get("stateCode"),
                    "country": venue.get("country", {}).get("countryCode"),
                    "coordinates": {
                        "lat": float(venue.get("location", {}).get("latitude", 0)),
                        "lng": float(venue.get("location", {}).get("longitude", 0))
                    }
                },
                "categories": [
                    segment.get("name")
                    for segment in raw_event.get("classifications", [{}])[0].get("segment", [])
                ],
                "price_info": {
                    "currency": priceRanges.get("currency", "USD"),
                    "min_price": float(priceRanges.get("min", 0)),
                    "max_price": float(priceRanges.get("max", 0)),
                    "price_tier": "premium" if priceRanges.get("max", 0) > 100 else "budget"
                },
                "source": {
                    "platform": self.platform,
                    "url": raw_event.get("url"),
                    "last_updated": datetime.utcnow().isoformat()
                }
            }
        except Exception as e:
            logger.error(f"Error standardizing event: {str(e)}")
            return None 