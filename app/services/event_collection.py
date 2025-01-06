from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncio
import logging
from sqlalchemy.orm import Session
from app.models.event import Event
from app.scrapers.eventbrite import EventbriteScraper
from app.scrapers.ticketmaster import TicketmasterScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.stubhub_scrapfly import StubhubScrapflyScraper
from app.scrapers.meta import MetaEventScraper
import os

logger = logging.getLogger(__name__)

class EventCollectionService:
    """Service to collect and store events from various sources"""
    
    def __init__(self):
        """Initialize the event collection service"""
        self.scrapers = []
        self._initialize_scrapers()
    
    def _initialize_scrapers(self):
        """Initialize available scrapers with API keys"""
        # Eventbrite
        eventbrite_key = os.getenv("EVENTBRITE_API_KEY")
        if eventbrite_key:
            self.scrapers.append(EventbriteScraper(eventbrite_key))
            
        # Ticketmaster
        ticketmaster_key = os.getenv("TICKETMASTER_API_KEY")
        if ticketmaster_key:
            self.scrapers.append(TicketmasterScraper(ticketmaster_key))
            
        # Meetup (using Scrapfly)
        scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
        if scrapfly_key:
            self.scrapers.append(MeetupScrapflyScraper(scrapfly_key))
            
        # StubHub (using Scrapfly)
        if scrapfly_key:  # Reuse same Scrapfly key
            self.scrapers.append(StubhubScrapflyScraper(scrapfly_key))
            
        # Meta (Facebook) Events
        meta_app_id = os.getenv("META_APP_ID")
        meta_app_secret = os.getenv("META_APP_SECRET")
        meta_access_token = os.getenv("META_ACCESS_TOKEN")
        if meta_app_id and meta_app_secret and meta_access_token:
            self.scrapers.append(MetaEventScraper(
                app_id=meta_app_id,
                app_secret=meta_app_secret,
                access_token=meta_access_token
            ))
    
    async def collect_events(
        self,
        db: Session,
        locations: List[Dict[str, Any]],
        date_range: Optional[Dict[str, datetime]] = None
    ) -> int:
        """
        Collect events from all sources for given locations and date range.
        Returns the number of new events collected.
        """
        if not date_range:
            # Default to events in the next 30 days
            date_range = {
                'start': datetime.utcnow(),
                'end': datetime.utcnow() + timedelta(days=30)
            }
        
        new_events_count = 0
        
        for scraper in self.scrapers:
            try:
                async with scraper:  # Use context manager for session handling
                    for location in locations:
                        events = await scraper.scrape_events(location, date_range)
                        new_events_count += await self._store_events(db, events)
            except Exception as e:
                logger.error(f"Error with {scraper.platform} scraper: {str(e)}")
                continue
        
        return new_events_count
    
    async def _store_events(self, db: Session, events: List[Dict[str, Any]]) -> int:
        """Store events in the database, returns number of new events stored"""
        new_events_count = 0
        
        for event_data in events:
            try:
                # Check if event already exists
                existing_event = db.query(Event).filter(
                    Event.source == event_data['source']['platform'],
                    Event.source_id == event_data['event_id']
                ).first()
                
                if existing_event:
                    # Update existing event
                    for key, value in event_data.items():
                        if key not in ['id', 'created_at']:
                            setattr(existing_event, key, value)
                    existing_event.updated_at = datetime.utcnow()
                else:
                    # Create new event
                    new_event = Event(
                        title=event_data['title'],
                        description=event_data['description'],
                        start_time=event_data['start_datetime'],
                        end_time=event_data['end_datetime'],
                        location=event_data['location']['coordinates'],
                        categories=event_data['categories'],
                        price_info=event_data['price_info'],
                        source=event_data['source']['platform'],
                        source_id=event_data['event_id'],
                        url=event_data['source']['url'],
                        image_url=event_data['images'][0] if event_data['images'] else None,
                        venue={
                            'name': event_data['location']['venue_name'],
                            'address': event_data['location']['address']
                        },
                        organizer=None,  # Will be populated by detailed scrape
                        tags=event_data['tags']
                    )
                    db.add(new_event)
                    new_events_count += 1
                
                db.commit()
                
            except Exception as e:
                logger.error(f"Error storing event {event_data.get('event_id')}: {str(e)}")
                db.rollback()
                continue
        
        return new_events_count 