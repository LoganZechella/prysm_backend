import os
import httpx
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from app.utils.schema import Event, Location, PriceInfo, SourceInfo
import json

logger = logging.getLogger(__name__)

class EventbriteCollector:
    """Collector for Eventbrite events"""
    
    def __init__(self):
        self.token = os.getenv("EVENTBRITE_PRIVATE_TOKEN")
        if not self.token:
            raise ValueError("EVENTBRITE_PRIVATE_TOKEN environment variable is not set")
        logger.info(f"Using Eventbrite private token: {self.token[:5]}...")  # Log first 5 chars for verification
        
        self.base_url = "https://www.eventbriteapi.com/v3"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        logger.info("Initialized Eventbrite collector")

    async def get_organization(self) -> Optional[str]:
        """Get the organization ID for the authenticated user"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/users/me/organizations/",
                headers=self.headers
            )
            
            if response.status_code != 200:
                logger.error(f"Error fetching organizations: {response.status_code} - {response.text}")
                return None
                
            data = response.json()
            logger.info(f"Organizations response: {json.dumps(data, indent=2)}")
            
            organizations = data.get("organizations", [])
            if not organizations:
                logger.error("No organizations found")
                return None
                
            return organizations[0]["id"]

    async def verify_token(self) -> bool:
        """Verify that the token works by calling /users/me endpoint"""
        async with httpx.AsyncClient() as client:
            # Try with Authorization header
            response = await client.get(
                f"{self.base_url}/users/me/",
                headers=self.headers
            )
            
            if response.status_code == 200:
                logger.info("Token verified successfully using Authorization header")
                return True
                
            # If that fails, try with token parameter
            response = await client.get(
                f"{self.base_url}/users/me/",
                params={"token": self.token}
            )
            
            if response.status_code == 200:
                logger.info("Token verified successfully using token parameter")
                return True
                
            logger.error(f"Token verification failed: {response.status_code} - {response.text}")
            logger.error(f"Response headers: {response.headers}")
            return False

    async def create_organization(self) -> Optional[str]:
        """Create a new organization if none exists"""
        async with httpx.AsyncClient() as client:
            # First check if we already have an organization
            response = await client.get(
                f"{self.base_url}/users/me/organizations/",
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                organizations = data.get("organizations", [])
                if organizations:
                    return organizations[0]["id"]
            
            # Create new organization
            org_data = {
                "name": "Event Collection Organization",
                "description": {
                    "text": "Organization for collecting event data"
                }
            }
            
            response = await client.post(
                f"{self.base_url}/organizations/",
                headers=self.headers,
                json=org_data
            )
            
            if response.status_code != 201:
                logger.error(f"Error creating organization: {response.status_code} - {response.text}")
                return None
                
            data = response.json()
            return data["id"]

    async def search_events(
        self,
        location: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        categories: Optional[List[str]] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """Search for events on Eventbrite using discovery API"""
        
        # First verify the token
        if not await self.verify_token():
            logger.error("Token verification failed, cannot proceed with search")
            return {"events": [], "pagination": {"has_more_items": False}}
        
        # Set default date range if not provided
        if not start_date:
            start_date = datetime.utcnow()
        if not end_date:
            end_date = start_date + timedelta(days=30)  # Default to next 30 days
            
        params = {
            "expand": "venue,ticket_availability,organizer,category",  # Include additional data
            "page": page,
            "sort_by": "date",  # Sort by date ascending
            "location.address": location if location else "San Francisco, CA",
            "location.within": "10km",  # Search within 10km radius
            "start_date.range_start": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start_date.range_end": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        if categories:
            params["categories"] = ",".join(categories)
            
        async with httpx.AsyncClient() as client:
            logger.info(f"Searching events with params: {params}")
            response = await client.get(
                f"{self.base_url}/discovery/v2/events/",
                headers=self.headers,
                params=params
            )
            
            if response.status_code != 200:
                logger.error(f"Eventbrite API error: {response.status_code} - {response.text}")
                logger.error(f"Request URL: {response.url}")
                return {"events": [], "pagination": {"has_more_items": False}}
                
            data = response.json()
            return data

    def transform_event(self, eventbrite_event: Dict[str, Any]) -> Event:
        """Transform Eventbrite event data into our standard Event schema"""
        
        # Extract venue information
        venue = eventbrite_event.get("venue", {})
        address = venue.get("address", {})
        
        # Extract price information
        ticket_info = eventbrite_event.get("ticket_availability", {})
        min_price = ticket_info.get("minimum_ticket_price", {}).get("value", 0)
        max_price = ticket_info.get("maximum_ticket_price", {}).get("value", min_price)
        
        # Determine price tier
        if min_price == 0:
            price_tier = "free"
        elif min_price < 20:
            price_tier = "budget"
        elif min_price < 50:
            price_tier = "medium"
        else:
            price_tier = "premium"
            
        return Event(
            event_id=f"eventbrite_{eventbrite_event['id']}",
            title=eventbrite_event["name"]["text"],
            description=eventbrite_event["description"]["text"],
            short_description=eventbrite_event["summary"],
            start_datetime=datetime.fromisoformat(eventbrite_event["start"]["utc"]),
            end_datetime=datetime.fromisoformat(eventbrite_event["end"]["utc"]),
            location=Location(
                venue_name=venue.get("name", ""),
                address=address.get("address_1", ""),
                city=address.get("city", ""),
                state=address.get("region", ""),
                country=address.get("country", ""),
                coordinates={
                    "lat": float(venue.get("latitude", 0)),
                    "lng": float(venue.get("longitude", 0))
                }
            ),
            categories=[cat["name"] for cat in eventbrite_event.get("categories", [])],
            tags=[tag["name"] for tag in eventbrite_event.get("tags", [])],
            price_info=PriceInfo(
                currency=ticket_info.get("currency", "USD"),
                min_price=min_price,
                max_price=max_price,
                price_tier=price_tier
            ),
            images=[img["url"] for img in eventbrite_event.get("images", [])],
            source=SourceInfo(
                platform="eventbrite",
                url=eventbrite_event["url"],
                last_updated=datetime.utcnow()
            )
        )

    async def collect_events(
        self,
        location: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        categories: Optional[List[str]] = None,
        max_pages: int = 5
    ) -> List[Event]:
        """Collect and transform events from Eventbrite"""
        all_events = []
        page = 1
        
        while page <= max_pages:
            response_data = await self.search_events(
                location=location,
                start_date=start_date,
                end_date=end_date,
                categories=categories,
                page=page
            )
            
            events = response_data.get("events", [])
            if not events:
                break
                
            # Transform each event
            for event_data in events:
                try:
                    event = self.transform_event(event_data)
                    all_events.append(event)
                except Exception as e:
                    logger.error(f"Error transforming event {event_data.get('id')}: {str(e)}")
                    continue
            
            # Check if there are more pages
            pagination = response_data.get("pagination", {})
            if not pagination.get("has_more_items", False):
                break
                
            page += 1
            
        return all_events

    async def get_categories(self) -> List[Dict[str, str]]:
        """Get list of available Eventbrite categories"""
        try:
            async with httpx.AsyncClient() as client:
                logger.info("Fetching Eventbrite categories...")
                response = await client.get(
                    f"{self.base_url}/categories/",
                    headers=self.headers
                )
                
                if response.status_code != 200:
                    logger.error(f"Error fetching categories: {response.status_code} - {response.text}")
                    logger.error(f"Request headers: {self.headers}")
                    logger.error(f"Response headers: {response.headers}")
                    return []
                    
                data = response.json()
                categories = data.get("categories", [])
                logger.info(f"Successfully fetched {len(categories)} categories")
                return categories
        except Exception as e:
            logger.error(f"Exception in get_categories: {str(e)}")
            return [] 