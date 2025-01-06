from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import httpx
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

class MetaEventScraper(BaseScraper):
    """Scraper for Meta (Facebook) events"""
    
    def __init__(self, app_id: str, app_secret: str, access_token: str):
        """Initialize the scraper with Meta API credentials"""
        self.platform = "meta"
        super().__init__(platform=self.platform)
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.base_url = "https://graph.facebook.com/v18.0"
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape Meta events for a given location and date range"""
        events = []
        
        try:
            # Search for places near the location
            places = await self._search_places(location)
            
            for place in places:
                # Get events for each place
                place_events = await self._get_place_events(
                    place['id'],
                    date_range['start'],
                    date_range['end']
                )
                
                for event in place_events:
                    try:
                        # Get detailed event info
                        event_data = await self._get_event_details(event['id'])
                        
                        if event_data:
                            events.append(event_data)
                            
                    except Exception as e:
                        logger.error(f"Error getting Meta event details: {str(e)}")
                        continue
            
        except Exception as e:
            logger.error(f"Error scraping Meta events: {str(e)}")
        
        return events
    
    async def _search_places(self, location: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search for places near the given location"""
        params = {
            'type': 'place',
            'center': f"{location['lat']},{location['lng']}",
            'distance': location.get('radius', 10) * 1000,  # Convert km to meters
            'limit': 50,
            'fields': 'id,name,location',
            'access_token': self.access_token
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.base_url}/search", params=params)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            return []
    
    async def _get_place_events(
        self,
        place_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Get events for a specific place"""
        params = {
            'fields': 'id,name',
            'since': int(start_time.timestamp()),
            'until': int(end_time.timestamp()),
            'access_token': self.access_token
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{place_id}/events",
                params=params
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            return []
    
    async def _get_event_details(self, event_id: str) -> Dict[str, Any]:
        """Get detailed information for an event"""
        params = {
            'fields': 'id,name,description,start_time,end_time,place,category,ticket_uri,cover',
            'access_token': self.access_token
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{event_id}",
                params=params
            )
            
            if response.status_code == 200:
                event = response.json()
                
                # Extract venue information
                venue = event.get('place', {})
                venue_location = venue.get('location', {})
                
                return {
                    'title': event.get('name'),
                    'description': event.get('description', ''),
                    'event_id': event['id'],
                    'source': {
                        'platform': self.platform,
                        'url': f"https://facebook.com/events/{event['id']}"
                    },
                    'start_datetime': datetime.fromisoformat(
                        event['start_time'].replace('Z', '+00:00')
                    ),
                    'end_datetime': datetime.fromisoformat(
                        event['end_time'].replace('Z', '+00:00')
                    ) if 'end_time' in event else None,
                    'location': {
                        'coordinates': {
                            'lat': float(venue_location.get('latitude', 0)),
                            'lng': float(venue_location.get('longitude', 0))
                        },
                        'venue_name': venue.get('name', ''),
                        'address': venue_location.get('street', '')
                    },
                    'categories': [event.get('category', 'Uncategorized')],
                    'price_info': {'is_free': None},  # Meta API doesn't provide price info
                    'images': [event['cover']['source']] if 'cover' in event else [],
                    'tags': []  # Meta API doesn't provide tags
                }
            
            return None 
    
    async def get_event_details(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific event"""
        params = {
            'fields': 'id,name,description,start_time,end_time,place,category,ticket_uri,cover',
            'access_token': self.access_token
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/{event_id}",
                params=params
            )
            
            if response.status_code == 200:
                event = response.json()
                
                # Extract venue information
                venue = event.get('place', {})
                venue_location = venue.get('location', {})
                
                return {
                    'title': event.get('name'),
                    'description': event.get('description', ''),
                    'event_id': event['id'],
                    'source': {
                        'platform': self.platform,
                        'url': f"https://facebook.com/events/{event['id']}"
                    },
                    'start_datetime': datetime.fromisoformat(
                        event['start_time'].replace('Z', '+00:00')
                    ),
                    'end_datetime': datetime.fromisoformat(
                        event['end_time'].replace('Z', '+00:00')
                    ) if 'end_time' in event else None,
                    'location': {
                        'coordinates': {
                            'lat': float(venue_location.get('latitude', 0)),
                            'lng': float(venue_location.get('longitude', 0))
                        },
                        'venue_name': venue.get('name', ''),
                        'address': venue_location.get('street', '')
                    },
                    'categories': [event.get('category', 'Uncategorized')],
                    'price_info': {'is_free': None},  # Meta API doesn't provide price info
                    'images': [event['cover']['source']] if 'cover' in event else [],
                    'tags': []  # Meta API doesn't provide tags
                }
            
            return None 