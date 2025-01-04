import os
import logging
from typing import Dict, List, Any, Optional, cast
from datetime import datetime, timedelta
import httpx
from app.utils.schema import Event, Location, PriceInfo, SourceInfo
from app.utils.category_hierarchy import create_default_hierarchy, map_platform_categories

logger = logging.getLogger(__name__)

class MetaCollector:
    """Collector for Meta (Facebook) events"""
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.base_url = "https://graph.facebook.com/v18.0"
        self.category_hierarchy = create_default_hierarchy()

    def _convert_to_event(self, event_data: Dict[str, Any]) -> Event:
        """Convert Meta event data to our Event model"""
        # Extract location information
        place = event_data.get("place", {})
        location = place.get("location", {})
        
        # Extract price information
        ticket_info = event_data.get("ticket_uri", {})
        price_info = PriceInfo()  # Default price info
        
        if "ticket_info" in event_data:
            price_info = PriceInfo(
                currency=event_data["ticket_info"].get("currency", "USD"),
                min_price=float(event_data["ticket_info"].get("min_price", 0)),
                max_price=float(event_data["ticket_info"].get("max_price", 0))
            )
        
        # Create Event object
        event = Event(
            event_id=event_data["id"],
            title=event_data["name"],
            description=event_data.get("description", ""),
            short_description=event_data.get("description", "")[:200] + "..." if len(event_data.get("description", "")) > 200 else event_data.get("description", ""),
            start_datetime=datetime.fromisoformat(event_data["start_time"].replace("Z", "+00:00")),
            end_datetime=datetime.fromisoformat(event_data["end_time"].replace("Z", "+00:00")) if "end_time" in event_data else None,
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
            price_info=price_info,
            source=SourceInfo(
                platform="meta",
                url=event_data.get("event_url", ""),
                last_updated=datetime.utcnow()
            )
        )
        
        # Store raw categories
        raw_categories = []
        if "category" in event_data:
            raw_categories.append(event_data["category"])
        if "event_type" in event_data:
            raw_categories.append(event_data["event_type"])
        event.raw_categories = raw_categories
        
        # Map categories to our hierarchy
        mapped_categories = map_platform_categories("meta", raw_categories, self.category_hierarchy)
        for category in mapped_categories:
            event.add_category(category, self.category_hierarchy)
        
        return event

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

    async def collect_events(
        self,
        location: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        categories: Optional[List[str]] = None
    ) -> List[Event]:
        """Collect events from Meta"""
        try:
            # Map our categories to Meta categories if provided
            meta_categories = []
            if categories:
                category_mapping = {}
                for node in self.category_hierarchy.nodes.values():
                    if "meta" in node.platform_mappings:
                        for meta_cat in node.platform_mappings["meta"]:
                            category_mapping[meta_cat.lower()] = node.name
                
                # Get available Meta categories
                available_cats = await self.get_categories()
                available_mapping = {cat["name"].lower(): cat["id"] for cat in available_cats}
                
                # Map our categories to Meta category IDs
                for category in categories:
                    if category in self.category_hierarchy.nodes:
                        node = self.category_hierarchy.nodes[category]
                        if "meta" in node.platform_mappings:
                            for meta_cat in node.platform_mappings["meta"]:
                                if meta_cat.lower() in available_mapping:
                                    meta_categories.append(available_mapping[meta_cat.lower()])
            
            # Build search parameters
            params = {
                "access_token": self.access_token,
                "fields": "id,name,description,start_time,end_time,place,ticket_uri,category,event_type,ticket_info"
            }
            
            if location:
                params["location"] = location
            if start_date:
                params["since"] = str(int(start_date.timestamp()))
            if end_date:
                params["until"] = str(int(end_date.timestamp()))
            if meta_categories:
                params["categories"] = ",".join(meta_categories)
            
            # Make API request
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/events/search",
                    params=params
                )
                
                if response.status_code != 200:
                    logger.error(f"Error searching events: {response.status_code} - {response.text}")
                    return []
                
                data = response.json()
                events = []
                
                for event_data in data.get("data", []):
                    try:
                        event = self._convert_to_event(event_data)
                        events.append(event)
                    except Exception as e:
                        logger.error(f"Error converting event {event_data.get('id')}: {str(e)}")
                        continue
                
                return events
                
        except Exception as e:
            logger.error(f"Exception in collect_events: {str(e)}")
            return [] 