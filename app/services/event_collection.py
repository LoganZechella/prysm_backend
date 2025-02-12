"""
Service for collecting events from various sources.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper
from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.services.event_pipeline import EventPipeline
from app.utils.retry_handler import RetryError
from app.models.event import EventModel
from app.db.session import AsyncSessionLocal

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
            async with AsyncSessionLocal() as db:
                # Collect from Eventbrite
                eventbrite_events = await self._collect_from_source(
                    db,
                    self.eventbrite,
                    "eventbrite",
                    locations,
                    date_range
                )
                total_events += len(eventbrite_events)
                
                # Collect from Facebook
                facebook_events = await self._collect_from_source(
                    db,
                    self.facebook,
                    "facebook",
                    locations,
                    date_range
                )
                total_events += len(facebook_events)
                
                # Collect from Meetup
                meetup_events = await self._collect_from_source(
                    db,
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
        db: AsyncSession,
        scraper: Any,
        source: str,
        locations: List[Dict[str, Any]],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Collect events from a single source"""
        all_events = []
        
        try:
            logger.info(f"Starting collection from {source}")
            for location in locations:
                try:
                    logger.info(f"Scraping {source} events for location: {location['city']}, {location['state']}")
                    # Scrape events
                    events = await scraper.scrape_events(
                        location=location,
                        date_range=date_range
                    )
                    
                    if events:
                        logger.info(f"Found {len(events)} events from {source}")
                        # Process and store events
                        stored_count = 0
                        for event in events:
                            try:
                                stored = await self.store_event(db, event, source)
                                if stored:
                                    stored_count += 1
                            except Exception as e:
                                logger.error(f"Error storing individual event from {source}: {str(e)}")
                                continue
                        
                        logger.info(f"Successfully stored {stored_count} events from {source}")
                        all_events.extend(events)
                    else:
                        logger.warning(f"No events found from {source} for location {location['city']}")
                        
                except Exception as e:
                    logger.error(f"Error collecting from {source} for location {location}: {str(e)}")
                    if hasattr(e, '__traceback__'):
                        import traceback
                        logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                    continue
                    
            logger.info(f"Completed collection from {source}, total events: {len(all_events)}")
            return all_events
            
        except Exception as e:
            logger.error(f"Fatal error in collection from {source}: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            return []
    
    def _get_default_location(self) -> Dict[str, Any]:
        """Get default location for searching"""
        return {
            "city": "Louisville",
            "state": "KY",
            "country": "US",
            "latitude": 38.2527,
            "longitude": -85.7585
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

    async def store_event(self, db: AsyncSession, event: Dict[str, Any], source: str) -> Optional[EventModel]:
        """Store an event in the database"""
        # Try different possible locations for event ID
        event_id = event.get("event_id") or event.get("id") or event.get("platform_id")
        if not event_id and event.get("url"):
            # Extract from URL as fallback
            event_id = event["url"].split("-")[-1]
            
        if not event_id:
            logger.error("Event ID is missing")
            return None

        try:
            # Check if event already exists
            result = await db.execute(
                select(EventModel).filter_by(
                    platform_id=event_id,
                    platform=source
                )
            )
            existing_event = result.scalar_one_or_none()

            if existing_event:
                logger.debug(f"Event {event_id} already exists in database")
                return None

            # Extract venue and organizer info with defaults
            venue = event.get("venue", {})
            if isinstance(venue, str):
                venue = {"name": venue}
            organizer = event.get("organizer", {})
            if isinstance(organizer, str):
                organizer = {"name": organizer}

            # Get venue information with defaults
            venue_name = (
                event.get("venue_name") or 
                venue.get("name") or 
                "Unknown Venue"
            )
            venue_lat = float(
                event.get("venue_lat") or 
                venue.get("lat") or 
                venue.get("latitude") or 
                0.0
            )
            venue_lon = float(
                event.get("venue_lon") or 
                venue.get("lon") or 
                venue.get("longitude") or 
                venue.get("lng") or 
                0.0
            )
            venue_city = (
                event.get("venue_city") or 
                venue.get("city") or 
                "Louisville"
            )
            venue_state = (
                event.get("venue_state") or 
                venue.get("state") or 
                "KY"
            )
            venue_country = (
                event.get("venue_country") or 
                venue.get("country") or 
                "US"
            )

            # Create new event
            event_model = EventModel(
                platform_id=event_id,
                platform=source,
                title=event.get("title"),
                description=event.get("description"),
                start_datetime=event.get("start_time") or event.get("start_datetime"),
                end_datetime=event.get("end_time") or event.get("end_datetime"),
                url=event.get("url"),
                
                # Venue information with defaults
                venue_name=venue_name,
                venue_lat=venue_lat,
                venue_lon=venue_lon,
                venue_city=venue_city,
                venue_state=venue_state,
                venue_country=venue_country,
                
                # Organizer information
                organizer_id=organizer.get("id") or event.get("organizer_id"),
                organizer_name=organizer.get("name") or event.get("organizer_name"),
                
                # Event metadata
                is_online=event.get("is_online", False),
                rsvp_count=int(event.get("rsvp_count", 0)),
                price_info=event.get("price_info") or event.get("prices", []),
                categories=event.get("categories", []),
                image_url=event.get("image_url")
            )

            db.add(event_model)
            await db.commit()
            await db.refresh(event_model)
            return event_model
            
        except Exception as e:
            logger.error(f"Error storing event {event_id}: {str(e)}")
            await db.rollback()
            return None 