"""
Service for collecting events from various sources.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import asyncio
from sqlalchemy.orm import Session

from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper
from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.services.event_pipeline import EventPipeline
from app.utils.retry_handler import RetryError

logger = logging.getLogger(__name__)

class EventCollectionService:
    """Service for collecting and storing events"""
    
    def __init__(self, scrapfly_api_key: str):
        """Initialize the collection service"""
        self.eventbrite = EventbriteScrapflyScraper(scrapfly_api_key)
        self.facebook = FacebookScrapflyScraper(scrapfly_api_key)
        self.meetup = MeetupScrapflyScraper(scrapfly_api_key)
        self.pipeline = EventPipeline()
        self.running = False
        
    async def collect_events(
        self,
        locations: Optional[List[Dict[str, Any]]] = None,
        date_range: Optional[Dict[str, datetime]] = None
    ) -> int:
        """
        Collect events from all sources.
        
        Args:
            locations: List of locations to search
            date_range: Date range to search
            
        Returns:
            Total number of events collected
        """
        if not locations:
            locations = [self._get_default_location()]
        if not date_range:
            date_range = self._get_default_date_range()
            
        total_events = 0
        
        try:
            # Collect from Eventbrite
            eventbrite_events = await self._collect_from_source(
                self.eventbrite,
                "eventbrite",
                locations,
                date_range
            )
            total_events += len(eventbrite_events)
            
            # Collect from Facebook
            facebook_events = await self._collect_from_source(
                self.facebook,
                "facebook",
                locations,
                date_range
            )
            total_events += len(facebook_events)
            
            # Collect from Meetup
            meetup_events = await self._collect_from_source(
                self.meetup,
                "meetup",
                locations,
                date_range
            )
            total_events += len(meetup_events)
            
            return total_events
            
        except Exception as e:
            logger.error(f"Error collecting events: {str(e)}")
            return total_events
    
    async def _collect_from_source(
        self,
        scraper: Any,
        source: str,
        locations: List[Dict[str, Any]],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Collect events from a single source"""
        all_events = []
        
        try:
            for location in locations:
                try:
                    # Scrape events
                    events = await scraper.scrape_events(
                        location=location,
                        date_range=date_range
                    )
                    
                    if events:
                        # Process and store events
                        processed_ids = await self.pipeline.process_events(events, source)
                        logger.info(f"Processed {len(processed_ids)} events from {source}")
                        
                        all_events.extend(events)
                        
                except Exception as e:
                    logger.error(f"Error collecting from {source} for location {location}: {str(e)}")
                    continue
                    
            return all_events
            
        except Exception as e:
            logger.error(f"Error in collection from {source}: {str(e)}")
            return []
    
    def _get_default_location(self) -> Dict[str, Any]:
        """Get default location for searching"""
        return {
            "city": "San Francisco",
            "state": "CA",
            "country": "US",
            "latitude": 37.7749,
            "longitude": -122.4194
        }
    
    def _get_default_date_range(self) -> Dict[str, datetime]:
        """Get default date range for searching"""
        now = datetime.utcnow()
        return {
            "start": now,
            "end": now + timedelta(days=30)
        }
    
    async def start_collection_task(
        self,
        interval_hours: int = 6,
        locations: Optional[List[Dict[str, Any]]] = None,
        date_range: Optional[Dict[str, datetime]] = None
    ) -> None:
        """Start continuous event collection task"""
        if self.running:
            logger.warning("Collection task is already running")
            return
            
        self.running = True
        while self.running:
            try:
                events_count = await self.collect_events(locations, date_range)
                logger.info(f"Collected {events_count} events")
                await asyncio.sleep(interval_hours * 3600)
            except Exception as e:
                logger.error(f"Error in collection task: {str(e)}")
                await asyncio.sleep(300)  # 5 minute delay on error
    
    def stop_collection_task(self) -> None:
        """Stop the collection task"""
        self.running = False 