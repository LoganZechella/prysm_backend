import os
import httpx
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
from app.utils.schema import Event, Location, PriceInfo, SourceInfo
import json
from app.utils.category_hierarchy import create_default_hierarchy, map_platform_categories

logger = logging.getLogger(__name__)

class EventbriteCollector:
    """Collector for Eventbrite events"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://www.eventbriteapi.com/v3"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.category_hierarchy = create_default_hierarchy()

    def _convert_to_event(self, event_data: Dict[str, Any]) -> Event:
        """Convert Eventbrite event data to our Event model"""
        # Extract venue information
        venue = event_data.get("venue", {})
        address = venue.get("address", {})
        
        # Extract price information
        ticket_info = event_data.get("ticket_classes", [{}])[0]
        min_price = float(ticket_info.get("cost", {}).get("actual", {}).get("value", 0))
        max_price = float(ticket_info.get("cost", {}).get("actual", {}).get("value", 0))
        currency = ticket_info.get("cost", {}).get("currency", "USD")
        
        # Create Event object
        event = Event(
            event_id=event_data["id"],
            title=event_data["name"]["text"],
            description=event_data["description"]["text"],
            short_description=event_data["description"]["text"][:200] + "..." if len(event_data["description"]["text"]) > 200 else event_data["description"]["text"],
            start_datetime=datetime.fromisoformat(event_data["start"]["utc"].replace("Z", "+00:00")),
            end_datetime=datetime.fromisoformat(event_data["end"]["utc"].replace("Z", "+00:00")),
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
            price_info=PriceInfo(
                currency=currency,
                min_price=min_price,
                max_price=max_price
            ),
            source=SourceInfo(
                platform="eventbrite",
                url=event_data["url"],
                last_updated=datetime.utcnow()
            )
        )
        
        # Store raw categories
        raw_categories = []
        for category in event_data.get("categories", []):
            if isinstance(category, dict) and "name" in category:
                raw_categories.append(category["name"])
        event.raw_categories = raw_categories
        
        # Map categories to our hierarchy
        mapped_categories = map_platform_categories("eventbrite", raw_categories, self.category_hierarchy)
        for category in mapped_categories:
            event.add_category(category, self.category_hierarchy)
        
        return event

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

    async def collect_events(
        self,
        location: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        categories: Optional[List[str]] = None
    ) -> List[Event]:
        """Collect events from Eventbrite"""
        try:
            # Map our categories to Eventbrite categories if provided
            eventbrite_categories = []
            if categories:
                category_mapping = {}
                for node in self.category_hierarchy.nodes.values():
                    if "eventbrite" in node.platform_mappings:
                        for eb_cat in node.platform_mappings["eventbrite"]:
                            category_mapping[eb_cat.lower()] = node.name
                
                # Get available Eventbrite categories
                available_cats = await self.get_categories()
                available_mapping = {cat["name"].lower(): cat["id"] for cat in available_cats}
                
                # Map our categories to Eventbrite category IDs
                for category in categories:
                    if category in self.category_hierarchy.nodes:
                        node = self.category_hierarchy.nodes[category]
                        if "eventbrite" in node.platform_mappings:
                            for eb_cat in node.platform_mappings["eventbrite"]:
                                if eb_cat.lower() in available_mapping:
                                    eventbrite_categories.append(available_mapping[eb_cat.lower()])
            
            # Build search parameters
            params = {
                "expand": "venue,ticket_classes,category",
            }
            
            if location:
                params["location.address"] = location
            if start_date:
                params["start_date.range_start"] = start_date.isoformat()
            if end_date:
                params["start_date.range_end"] = end_date.isoformat()
            if eventbrite_categories:
                params["categories"] = ",".join(eventbrite_categories)
            
            # Make API request
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/events/search/",
                    headers=self.headers,
                    params=params
                )
                
                if response.status_code != 200:
                    logger.error(f"Error searching events: {response.status_code} - {response.text}")
                    return []
                
                data = response.json()
                events = []
                
                for event_data in data.get("events", []):
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