import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from urllib.parse import urlencode
from .base import BaseScraper

logger = logging.getLogger(__name__)

class EventbriteScraper(BaseScraper):
    """Scraper for Eventbrite events"""
    
    def __init__(self, api_key: str):
        """Initialize the Eventbrite scraper"""
        super().__init__("eventbrite")
        self.api_key = api_key
        self.base_url = "https://www.eventbriteapi.com/v3"
        self.headers.update({
            'Authorization': f'Bearer {api_key}'
        })
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Optional[Dict[str, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """Scrape events from Eventbrite for a given location and date range"""
        try:
            # Build search parameters
            params = {
                'location.latitude': location.get('lat'),
                'location.longitude': location.get('lng'),
                'location.within': '10km',
                'expand': 'venue,ticket_availability'
            }
            
            if date_range:
                params.update({
                    'start_date.range_start': date_range['start'].isoformat(),
                    'start_date.range_end': date_range['end'].isoformat()
                })
            
            # Fetch events
            url = f"{self.base_url}/events/search/?{urlencode(params)}"
            response_text = await self.fetch_page(url)
            
            if not response_text:
                return []
            
            data = json.loads(response_text)
            events = []
            
            for event_data in data.get('events', []):
                try:
                    # Get detailed event information
                    event_details = await self.get_event_details(event_data['id'])
                    if event_details:
                        events.append(self.standardize_event(event_details))
                except Exception as e:
                    logger.error(f"Error processing event {event_data.get('id')}: {str(e)}")
                    continue
            
            return events
            
        except Exception as e:
            logger.error(f"Error scraping Eventbrite events: {str(e)}")
            return []
    
    async def get_event_details(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific Eventbrite event"""
        try:
            url = f"{self.base_url}/events/{event_id}/?expand=venue,ticket_classes,category"
            response_text = await self.fetch_page(url)
            
            if not response_text:
                return None
            
            event_data = json.loads(response_text)
            
            # Extract relevant information
            venue = event_data.get('venue', {})
            category = event_data.get('category', {})
            ticket_classes = event_data.get('ticket_classes', [])
            
            # Calculate price range
            prices = [
                float(ticket['cost']['major_value'])
                for ticket in ticket_classes
                if ticket.get('cost') and not ticket.get('free')
            ]
            
            min_price = min(prices) if prices else 0.0
            max_price = max(prices) if prices else 0.0
            
            # Determine price tier
            if not prices:
                price_tier = 'free'
            elif max_price <= 20:
                price_tier = 'budget'
            elif max_price <= 50:
                price_tier = 'medium'
            else:
                price_tier = 'premium'
            
            return {
                'id': event_data['id'],
                'title': event_data['name']['text'],
                'description': event_data['description']['text'],
                'start_datetime': event_data['start']['utc'],
                'end_datetime': event_data['end']['utc'],
                'url': event_data['url'],
                'venue': {
                    'name': venue.get('name', ''),
                    'address': venue.get('address', {}).get('localized_address_display', ''),
                    'city': venue.get('address', {}).get('city', ''),
                    'state': venue.get('address', {}).get('region', ''),
                    'country': venue.get('address', {}).get('country', ''),
                    'coordinates': {
                        'lat': float(venue.get('latitude', 0)),
                        'lng': float(venue.get('longitude', 0))
                    }
                },
                'categories': [category.get('name', '')],
                'tags': [tag['display_name'] for tag in event_data.get('tags', [])],
                'price': {
                    'currency': 'USD',
                    'min': min_price,
                    'max': max_price,
                    'tier': price_tier
                },
                'images': [img['url'] for img in event_data.get('logo', {}).values() if isinstance(img, dict)]
            }
            
        except Exception as e:
            logger.error(f"Error getting event details for {event_id}: {str(e)}")
            return None 
    
    def standardize_event(self, raw_event: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize event data to match our schema"""
        try:
            # Calculate price tier
            min_price = raw_event.get('price', {}).get('min', 0.0)
            max_price = raw_event.get('price', {}).get('max', 0.0)
            
            if max_price == 0.0:
                price_tier = 'free'
            elif max_price <= 20:
                price_tier = 'budget'
            elif max_price <= 50:
                price_tier = 'medium'
            else:
                price_tier = 'premium'
            
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
                    "min_price": min_price,
                    "max_price": max_price,
                    "price_tier": price_tier
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