import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import asyncio
from .base import BaseScraper

logger = logging.getLogger(__name__)

class MeetupScraper(BaseScraper):
    """Scraper for Meetup events using their REST API"""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the scraper with API key"""
        super().__init__(platform="meetup")
        self.api_key = api_key or os.getenv("MEETUP_API_KEY")
        if not self.api_key:
            raise ValueError("Meetup API key is required")
        self.base_url = "https://api.meetup.com"
        self.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'Accept': 'application/json'
        })
        
    async def scrape_events(self, location: Dict[str, Any], date_range: Optional[Dict[str, datetime]] = None) -> List[Dict[str, Any]]:
        """Scrape events for a given location and date range"""
        events = []
        
        # Build query parameters
        params = {
            'location': f"{location.get('city')}, {location.get('state')}",
            'country': location.get('country', 'US'),
            'radius': 'smart',  # Meetup's adaptive radius based on location density
            'status': 'upcoming',
            'page': 200  # Maximum allowed per request
        }
        
        if date_range:
            params.update({
                'no_earlier_than': date_range['start'].strftime('%Y-%m-%dT%H:%M:%S'),
                'no_later_than': date_range['end'].strftime('%Y-%m-%dT%H:%M:%S')
            })
        
        url = f"{self.base_url}/find/upcoming_events"
        
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    raw_events = data.get('events', [])
                    
                    # Process events in parallel
                    tasks = [self.get_event_details(event['id']) for event in raw_events]
                    detailed_events = await asyncio.gather(*tasks)
                    
                    # Filter out None results and add valid events
                    events.extend([event for event in detailed_events if event])
                else:
                    logger.error(f"Failed to fetch events: {response.status}")
        
        except Exception as e:
            logger.error(f"Error scraping events: {str(e)}")
        
        return events
    
    async def get_event_details(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific event"""
        url = f"{self.base_url}/events/{event_id}"
        
        try:
            if not self.session:
                return None
                
            async with self.session.get(url) as response:
                if response.status == 200:
                    raw_event = await response.json()
                    return self.standardize_event(raw_event)
                else:
                    logger.error(f"Failed to fetch event details: {response.status}")
        except Exception as e:
            logger.error(f"Error getting event details: {str(e)}")
        
        return None
    
    def standardize_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Meetup event format to our standard schema"""
        try:
            # Extract venue information
            venue = raw_event.get('venue', {})
            group = raw_event.get('group', {})
            
            # Extract fee information
            fee = raw_event.get('fee', {})
            
            # Build standardized event
            return {
                "event_id": f"mu_{raw_event['id']}",
                "title": raw_event.get('name'),
                "description": raw_event.get('description', ''),
                "start_datetime": raw_event.get('time'),
                "end_datetime": raw_event.get('end_time'),
                "location": {
                    "venue_name": venue.get('name', group.get('name')),
                    "address": venue.get('address_1'),
                    "city": venue.get('city'),
                    "state": venue.get('state'),
                    "country": venue.get('country', 'US'),
                    "coordinates": {
                        "lat": float(venue.get('lat', 0)),
                        "lng": float(venue.get('lon', 0))
                    }
                },
                "categories": [
                    topic.get('name')
                    for topic in group.get('topics', [])
                ],
                "price_info": {
                    "currency": fee.get('currency', 'USD'),
                    "min_price": float(fee.get('amount', 0)),
                    "max_price": float(fee.get('amount', 0)),
                    "price_tier": "premium" if fee.get('amount', 0) > 50 else "budget"
                },
                "source": {
                    "platform": self.platform,
                    "url": raw_event.get('link'),
                    "last_updated": datetime.utcnow().isoformat()
                },
                "attributes": {
                    "group_name": group.get('name'),
                    "is_online": raw_event.get('is_online_event', False),
                    "rsvp_limit": raw_event.get('rsvp_limit'),
                    "waitlist_count": raw_event.get('waitlist_count', 0),
                    "yes_rsvp_count": raw_event.get('yes_rsvp_count', 0)
                }
            }
        except Exception as e:
            logger.error(f"Error standardizing event: {str(e)}")
            return None 