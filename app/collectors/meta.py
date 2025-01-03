import os
import logging
from typing import Dict, List, Any, Optional, cast
from datetime import datetime, timedelta
import httpx
from app.models.event import Event, Location, PriceInfo, SourceInfo

logger = logging.getLogger(__name__)

class MetaCollector:
    """Collector for Meta (Facebook) events"""
    
    def __init__(self):
        self.access_token = os.getenv("META_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("META_ACCESS_TOKEN environment variable is not set")
        logger.info(f"Using Meta access token: {self.access_token[:5]}...")
        
        self.app_id = os.getenv("META_APP_ID")
        self.app_secret = os.getenv("META_APP_SECRET")
        if not self.app_id or not self.app_secret:
            raise ValueError("META_APP_ID and META_APP_SECRET environment variables must be set")
        
        self.base_url = "https://graph.facebook.com/v19.0"  # Using latest stable version
        # Don't set Authorization header, we'll pass token as a parameter
        self.headers = {
            "Accept": "application/json"
        }
        logger.info("Initialized Meta collector")
        
    async def verify_token(self) -> bool:
        """Verify that the access token is valid"""
        async with httpx.AsyncClient() as client:
            # Pass token as a parameter instead of header
            response = await client.get(
                f"{self.base_url}/me",
                params={
                    "access_token": self.access_token,
                    "fields": "id,name"
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Token verification failed: {response.status_code} - {response.text}")
                return False
                
            data = response.json()
            if "id" not in data:
                logger.error(f"Token is invalid: {data}")
                return False
                
            logger.info(f"Token verified successfully for user {data.get('name', 'Unknown')}")
            return True
            
    async def search_events(
        self,
        location: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        categories: Optional[List[str]] = None,
        page: Optional[str] = None  # Using string as Meta uses cursors for pagination
    ) -> Dict[str, Any]:
        """Search for events using Meta Graph API"""
        
        # First verify the token
        if not await self.verify_token():
            logger.error("Token verification failed, cannot proceed with search")
            return {"data": [], "paging": {}}
        
        # Set default date range if not provided
        if not start_date:
            start_date = datetime.utcnow()
        if not end_date:
            end_date = start_date + timedelta(days=30)
            
        # Build the fields parameter to get all needed event data
        fields = [
            "id",
            "name",
            "description",
            "start_time",
            "end_time",
            "place",
            "category",
            "ticket_uri",
            "cover",
            "attending_count",
            "interested_count",
            "is_online",
            "event_times",
            "ticket_tiers"
        ]
        
        async with httpx.AsyncClient() as client:
            all_events = []
            
            # First try to get events from the user's pages
            logger.info("Getting user's pages")
            pages_response = await client.get(
                f"{self.base_url}/me/accounts",
                params={
                    "access_token": self.access_token,
                    "fields": "id,name,access_token",
                    "limit": "10"  # Limit to 10 pages for now
                }
            )
            
            if pages_response.status_code == 200:
                pages_data = pages_response.json()
                pages = pages_data.get("data", [])
                logger.info(f"Found {len(pages)} pages")
                
                # Get events for each page
                for page_info in pages:
                    page_id = page_info["id"]
                    page_token = page_info.get("access_token", self.access_token)
                    
                    events_params = {
                        "access_token": page_token,
                        "fields": ",".join(fields),
                        "time_filter": "upcoming"
                    }
                    
                    if page:
                        events_params["after"] = page
                        
                    logger.info(f"Getting events for page {page_info['name']} ({page_id})")
                    events_response = await client.get(
                        f"{self.base_url}/{page_id}/events",
                        params=events_params
                    )
                    
                    if events_response.status_code == 200:
                        events_data = events_response.json()
                        page_events = events_data.get("data", [])
                        logger.info(f"Found {len(page_events)} events for page {page_info['name']}")
                        all_events.extend(page_events)
                    else:
                        logger.warning(f"Failed to get events for page {page_id}: {events_response.status_code} - {events_response.text}")
            else:
                logger.error(f"Meta API error getting pages: {pages_response.status_code} - {pages_response.text}")
            
            # Then try to get events from the user's groups
            logger.info("Getting user's groups")
            groups_response = await client.get(
                f"{self.base_url}/me/groups",
                params={
                    "access_token": self.access_token,
                    "fields": "id,name",
                    "limit": "10"  # Limit to 10 groups for now
                }
            )
            
            if groups_response.status_code == 200:
                groups_data = groups_response.json()
                groups = groups_data.get("data", [])
                logger.info(f"Found {len(groups)} groups")
                
                # Get events for each group
                for group in groups:
                    group_id = group["id"]
                    events_params = {
                        "access_token": self.access_token,
                        "fields": ",".join(fields),
                        "time_filter": "upcoming"
                    }
                    
                    if page:
                        events_params["after"] = page
                        
                    logger.info(f"Getting events for group {group['name']} ({group_id})")
                    events_response = await client.get(
                        f"{self.base_url}/{group_id}/events",
                        params=events_params
                    )
                    
                    if events_response.status_code == 200:
                        events_data = events_response.json()
                        group_events = events_data.get("data", [])
                        logger.info(f"Found {len(group_events)} events for group {group['name']}")
                        all_events.extend(group_events)
                    else:
                        logger.warning(f"Failed to get events for group {group_id}: {events_response.status_code} - {events_response.text}")
            else:
                logger.error(f"Meta API error getting groups: {groups_response.status_code} - {groups_response.text}")
            
            return {"data": all_events, "paging": {}}
            
    def transform_event(self, meta_event: Dict[str, Any]) -> Event:
        """Transform Meta event data into our standard Event schema"""
        
        # Extract place information
        place = meta_event.get("place", {})
        location = place.get("location", {})
        
        # Extract price information from ticket tiers if available
        ticket_tiers = meta_event.get("ticket_tiers", [])
        if ticket_tiers:
            prices = [float(tier.get("price", {}).get("amount", 0)) for tier in ticket_tiers]
            min_price = min(prices) if prices else 0
            max_price = max(prices) if prices else 0
        else:
            min_price = 0
            max_price = 0
            
        # Determine price tier
        if min_price == 0:
            price_tier = "free"
        elif min_price < 20:
            price_tier = "budget"
        elif min_price < 50:
            price_tier = "medium"
        else:
            price_tier = "premium"
            
        # Get cover image if available
        images = []
        if cover := meta_event.get("cover", {}):
            if cover_url := cover.get("source"):
                images.append(cover_url)
                
        # Handle potential missing required fields
        name = meta_event.get("name", "Untitled Event")
        start_time = meta_event.get("start_time")
        if not start_time:
            raise ValueError(f"Event {meta_event.get('id')} missing required start_time field")
                
        return Event(
            event_id=f"meta_{meta_event['id']}",
            title=name,
            description=meta_event.get("description", ""),
            short_description=meta_event.get("description", "")[:200] + "..." if meta_event.get("description") else "",
            start_datetime=datetime.fromisoformat(start_time.replace("Z", "+00:00")),
            end_datetime=datetime.fromisoformat(meta_event["end_time"].replace("Z", "+00:00")) if meta_event.get("end_time") else None,
            location=Location(
                venue_name=place.get("name", ""),
                address=location.get("street", ""),
                city=location.get("city", ""),
                state=location.get("state", ""),
                country=location.get("country", ""),
                coordinates={
                    "lat": float(location.get("latitude", 0)),
                    "lng": float(location.get("longitude", 0))
                }
            ),
            categories=[meta_event.get("category", "Uncategorized")],
            tags=[],  # Meta events don't have tags
            price_info=PriceInfo(
                currency="USD",  # Meta events typically use local currency
                min_price=min_price,
                max_price=max_price,
                price_tier=price_tier
            ),
            images=images,
            source=SourceInfo(
                platform="meta",
                url=meta_event.get("ticket_uri", ""),  # Use ticket URL if available
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
        """Collect and transform events from Meta"""
        all_events = []
        next_page = None
        page_count = 0
        
        while page_count < max_pages:
            response_data = await self.search_events(
                location=location,
                start_date=start_date,
                end_date=end_date,
                categories=categories,
                page=next_page
            )
            
            events = response_data.get("data", [])
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
            paging = response_data.get("paging", {})
            cursors = paging.get("cursors", {})
            next_page = cursors.get("after")
            if not next_page:
                break
                
            page_count += 1
            
        return all_events 

    async def get_long_lived_token(self) -> Optional[str]:
        """Exchange short-lived token for a long-lived one"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/oauth/access_token",
                params={
                    "grant_type": "fb_exchange_token",
                    "client_id": self.app_id,
                    "client_secret": self.app_secret,
                    "fb_exchange_token": self.access_token,
                    "access_token": f"{self.app_id}|{self.app_secret}"  # Use app access token for auth
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get long-lived token: {response.status_code} - {response.text}")
                return None
                
            data = response.json()
            new_token = data.get("access_token")
            if not new_token:
                logger.error("No access token in response")
                return None
                
            logger.info("Successfully obtained long-lived token")
            return new_token
            
    async def initialize(self) -> bool:
        """Initialize the collector with a long-lived token"""
        # First try to verify if current token is valid
        if await self.verify_token():
            logger.info("Current token is valid, skipping token exchange")
            return True
            
        # If current token is not valid, try to exchange it for a long-lived one
        long_lived_token = await self.get_long_lived_token()
        if long_lived_token:
            self.access_token = long_lived_token
            self.headers["Authorization"] = f"Bearer {self.access_token}"
            return True
        return False

    async def get_categories(self) -> List[Dict[str, Any]]:
        """Get list of available event categories"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/event_categories",
                params={
                    "access_token": self.access_token
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get categories: {response.status_code} - {response.text}")
                return []
                
            data = response.json()
            categories = data.get("data", [])
            logger.info(f"Found {len(categories)} categories")
            return categories 