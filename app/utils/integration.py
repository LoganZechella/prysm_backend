import logging
from typing import List, Optional
from datetime import datetime
from app.collectors.meta import MetaCollector
from app.collectors.eventbrite import EventbriteCollector
from app.collectors.scraper import EventScraper
from app.utils.event_processing import process_event
from app.utils.storage import StorageManager
from app.utils.schema import Event, Location, PriceInfo, SourceInfo, EventAttributes
import json

logger = logging.getLogger(__name__)

class EventIntegration:
    """Integration layer between collectors and processing pipeline"""
    
    def __init__(self):
        self.meta_collector = MetaCollector()
        self.eventbrite_collector = EventbriteCollector()
        self.scraper = EventScraper()
        self.storage = StorageManager()
        
    async def process_event(self, event: Event) -> Event:
        """Process an event by enriching it with additional data"""
        # TODO: Add event processing logic
        # For now, just return the event as is
        return event

    async def collect_and_process_events(
        self,
        location: str,
        start_date: datetime,
        end_date: datetime,
        categories: Optional[List[str]] = None
    ) -> List[Event]:
        """Collect and process events from all sources"""
        all_events = []
        
        # Collect events from Meta
        try:
            meta_events = await self.meta_collector.collect_events(
                location=location,
                start_date=start_date,
                end_date=end_date,
                categories=categories
            )
            
            # Process and store each event
            for event in meta_events:
                try:
                    # Store raw event data
                    raw_data = json.loads(event.model_dump_json())
                    try:
                        await self.storage.store_raw_event("meta", raw_data)
                    except Exception as storage_error:
                        logger.warning(f"Failed to store raw meta event: {str(storage_error)}")
                    
                    # Process event
                    processed_event = await self.process_event(event)
                    
                    # Store processed event data
                    processed_data = json.loads(processed_event.model_dump_json())
                    try:
                        await self.storage.store_processed_event(processed_data)
                    except Exception as storage_error:
                        logger.warning(f"Failed to store processed meta event: {str(storage_error)}")
                    
                    all_events.append(processed_event)
                except Exception as e:
                    logger.error(f"Error processing meta event {event.event_id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error collecting Meta events: {str(e)}")
            
        # Collect events from Eventbrite
        try:
            eventbrite_events = await self.eventbrite_collector.collect_events(
                location=location,
                start_date=start_date,
                end_date=end_date,
                categories=categories
            )
            
            # Process and store each event
            for event in eventbrite_events:
                try:
                    # Store raw event data
                    raw_data = json.loads(event.model_dump_json())
                    try:
                        await self.storage.store_raw_event("eventbrite", raw_data)
                    except Exception as storage_error:
                        logger.warning(f"Failed to store raw eventbrite event: {str(storage_error)}")
                    
                    # Process event
                    processed_event = await self.process_event(event)
                    
                    # Store processed event data
                    processed_data = json.loads(processed_event.model_dump_json())
                    try:
                        await self.storage.store_processed_event(processed_data)
                    except Exception as storage_error:
                        logger.warning(f"Failed to store processed eventbrite event: {str(storage_error)}")
                    
                    all_events.append(processed_event)
                except Exception as e:
                    logger.error(f"Error processing eventbrite event {event.event_id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error collecting Eventbrite events: {str(e)}")
            
        # Collect events from web scraping
        try:
            sources = ["eventbrite"]  # Add more sources as needed
            async for event in self.scraper.scrape_events(
                sources=sources,
                location=location,
                start_date=start_date,
                end_date=end_date
            ):
                try:
                    # Store raw event data
                    raw_data = json.loads(event.model_dump_json())
                    try:
                        await self.storage.store_raw_event("scraper", raw_data)
                    except Exception as storage_error:
                        logger.warning(f"Failed to store raw scraped event: {str(storage_error)}")
                    
                    # Process event
                    processed_event = await self.process_event(event)
                    
                    # Store processed event data
                    processed_data = json.loads(processed_event.model_dump_json())
                    try:
                        await self.storage.store_processed_event(processed_data)
                    except Exception as storage_error:
                        logger.warning(f"Failed to store processed scraped event: {str(storage_error)}")
                    
                    all_events.append(processed_event)
                except Exception as e:
                    logger.error(f"Error processing scraped event {event.event_id}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error collecting scraped events: {str(e)}")
            
        return all_events
    
    async def get_processed_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Event]:
        """Retrieve processed events from storage"""
        events = []
        
        try:
            # List events from the specified time range
            year = start_date.year if start_date else None
            month = start_date.month if start_date else None
            
            blob_names = await self.storage.list_processed_events(year, month)
            
            for blob_name in blob_names:
                try:
                    event_data = await self.storage.get_processed_event(blob_name)
                    if event_data:
                        event = Event.from_raw_data(event_data)
                        
                        # Filter by date range if specified
                        if start_date and event.start_datetime < start_date:
                            continue
                        if end_date and event.start_datetime > end_date:
                            continue
                            
                        events.append(event)
                        
                except Exception as e:
                    logger.error(f"Error loading event from {blob_name}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error listing processed events: {str(e)}")
        
        return events 