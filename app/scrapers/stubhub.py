import os
import base64
import urllib.parse
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)

class StubHubScraper(BaseScraper):
    """Scraper for StubHub events using their API"""
    
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None):
        """Initialize the scraper with client credentials"""
        super().__init__(platform="stubhub")
        self.client_id = client_id or os.getenv("STUBHUB_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("STUBHUB_CLIENT_SECRET")
        if not self.client_id or not self.client_secret:
            raise ValueError("StubHub client ID and secret are required")
        
        self.base_url = "https://api.stubhub.com"
        self.access_token = None
        self.headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    
    def _create_basic_auth_header(self) -> str:
        """Create Basic Authorization header for token request"""
        if not isinstance(self.client_id, str) or not isinstance(self.client_secret, str):
            raise ValueError("Client ID and secret must be strings")
            
        # URL encode credentials
        encoded_id = urllib.parse.quote(self.client_id)
        encoded_secret = urllib.parse.quote(self.client_secret)
        
        # Concatenate with colon
        credentials = f"{encoded_id}:{encoded_secret}"
        
        # Base64 encode
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        return f"Basic {encoded_credentials}"
    
    async def _get_access_token(self) -> bool:
        """Obtain an access token using client credentials"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use 'async with' context manager.")
        
        url = f"{self.base_url}/sellers/oauth/token"
        
        headers = {
            'Authorization': self._create_basic_auth_header(),
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {
            'grant_type': 'client_credentials',
            'scope': 'read:events'
        }
        
        try:
            async with self.session.post(url, headers=headers, data=data) as response:
                if response.status == 200:
                    token_data = await response.json()
                    self.access_token = token_data['access_token']
                    # Update headers with new bearer token
                    self.headers.update({
                        'Authorization': f"Bearer {self.access_token}",
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'
                    })
                    return True
                else:
                    logger.error(f"Failed to get access token: {response.status}")
                    logger.error(f"Response: {await response.text()}")
                    return False
        except Exception as e:
            logger.error(f"Error getting access token: {str(e)}")
            return False
    
    async def scrape_events(self, location: Dict[str, Any], date_range: Optional[Dict[str, datetime]] = None) -> List[Dict[str, Any]]:
        """Scrape events for a given location and date range"""
        events = []
        
        # Get access token if we don't have one
        if not self.access_token:
            if not await self._get_access_token():
                return events
        
        # Build query parameters
        params = {
            'city': location.get('city'),
            'state': location.get('state'),
            'country': location.get('country', 'US'),
            'rows': 100  # Number of results per page
        }
        
        if date_range:
            params.update({
                'dateLocal.gte': date_range['start'].strftime('%Y-%m-%dT%H:%M:%S'),
                'dateLocal.lte': date_range['end'].strftime('%Y-%m-%dT%H:%M:%S')
            })
        
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            url = f"{self.base_url}/sellers/search/events/v3"
            async with self.session.get(url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    events_data = data.get('events', [])
                    
                    # Process each event
                    for event_data in events_data:
                        event = await self.get_event_details(event_data['id'])
                        if event:
                            events.append(event)
                elif response.status == 401:
                    # Token might be expired, try to get a new one
                    if await self._get_access_token():
                        # Retry the request with new token
                        return await self.scrape_events(location, date_range)
                else:
                    logger.error(f"Failed to fetch events: {response.status}")
                    logger.error(f"Response: {await response.text()}")
        
        except Exception as e:
            logger.error(f"Error scraping events: {str(e)}")
        
        return events
    
    async def get_event_details(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific event"""
        if not self.access_token:
            if not await self._get_access_token():
                return None
        
        url = f"{self.base_url}/sellers/search/events/v3/{event_id}"
        
        try:
            if not self.session:
                return None
            
            async with self.session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    event_data = await response.json()
                    return self.standardize_event(event_data)
                elif response.status == 401:
                    # Token might be expired, try to get a new one
                    if await self._get_access_token():
                        # Retry the request with new token
                        return await self.get_event_details(event_id)
                else:
                    logger.error(f"Failed to fetch event details: {response.status}")
        except Exception as e:
            logger.error(f"Error getting event details: {str(e)}")
        
        return None
    
    def standardize_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert StubHub event format to our standard schema"""
        try:
            # Extract venue information
            venue = raw_event.get('venue', {})
            
            # Extract price information
            price_info = raw_event.get('ticketInfo', {})
            min_price = float(price_info.get('minPrice', 0))
            max_price = float(price_info.get('maxPrice', 0))
            
            # Determine price tier
            if max_price == 0:
                price_tier = "free"
            elif max_price <= 50:
                price_tier = "budget"
            elif max_price <= 150:
                price_tier = "medium"
            else:
                price_tier = "premium"
            
            # Build standardized event
            return {
                "event_id": f"sh_{raw_event['id']}",
                "title": raw_event.get('name'),
                "description": raw_event.get('description', ''),
                "start_datetime": raw_event.get('eventDateLocal'),
                "end_datetime": raw_event.get('eventEndDateLocal'),
                "location": {
                    "venue_name": venue.get('name'),
                    "address": venue.get('address', {}).get('street1'),
                    "city": venue.get('address', {}).get('city'),
                    "state": venue.get('address', {}).get('state'),
                    "country": venue.get('address', {}).get('country'),
                    "coordinates": {
                        "lat": float(venue.get('latitude', 0)),
                        "lng": float(venue.get('longitude', 0))
                    }
                },
                "categories": [
                    raw_event.get('categoryName', ''),
                    raw_event.get('subcategoryName', '')
                ],
                "price_info": {
                    "currency": price_info.get('currency', 'USD'),
                    "min_price": min_price,
                    "max_price": max_price,
                    "price_tier": price_tier
                },
                "source": {
                    "platform": self.platform,
                    "url": raw_event.get('eventUrl'),
                    "last_updated": datetime.utcnow().isoformat()
                },
                "attributes": {
                    "total_tickets": raw_event.get('totalTickets', 0),
                    "min_quantity": raw_event.get('minQuantity', 1),
                    "max_quantity": raw_event.get('maxQuantity', 1),
                    "delivery_methods": raw_event.get('deliveryMethods', [])
                }
            }
            
        except Exception as e:
            logger.error(f"Error standardizing event: {str(e)}")
            return None 