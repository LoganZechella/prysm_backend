"""
Event Collection Service

This module provides a unified service for collecting events from various sources.
It handles scraper initialization, event collection, and storage in a centralized way.
"""

from typing import List, Dict, Any, Optional, Type
from datetime import datetime, timedelta
import asyncio
import logging
import os
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.event import Event
from app.db.session import SessionLocal
from app.scrapers.base import ScrapflyBaseScraper
from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper

logger = logging.getLogger(__name__)

class EventCollectionService:
    """Unified service for event collection from multiple sources"""
    
    # Default locations if none provided
    DEFAULT_LOCATIONS = [
        {
            'city': 'San Francisco',
            'state': 'California',
            'latitude': 37.7749,
            'longitude': -122.4194,
            'radius': 10
        },
        {
            'city': 'New York',
            'state': 'New York',
            'latitude': 40.7128,
            'longitude': -74.0060,
            'radius': 10
        }
    ]
    
    def __init__(self):
        """Initialize the event collection service"""
        self.scrapers = []
        self.running = False
        self._initialize_scrapers()
        
    def _initialize_scrapers(self) -> None:
        """Initialize all available scrapers"""
        scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
        if not scrapfly_key:
            raise ValueError("SCRAPFLY_API_KEY environment variable not set")
            
        scraper_classes = [
            EventbriteScrapflyScraper,
            MeetupScrapflyScraper,
            FacebookScrapflyScraper
        ]
        
        for scraper_class in scraper_classes:
            try:
                scraper = scraper_class(scrapfly_key)
                self.scrapers.append(scraper)
                logger.info(f"Initialized {scraper.platform} scraper")
            except Exception as e:
                logger.error(f"Failed to initialize {scraper_class.__name__}: {str(e)}")
                
        if not self.scrapers:
            raise RuntimeError("No scrapers were successfully initialized")
            
    async def collect_events(
        self,
        locations: Optional[List[Dict[str, Any]]] = None,
        date_range: Optional[Dict[str, datetime]] = None,
        categories: Optional[List[str]] = None
    ) -> int:
        """
        Collect events from all sources
        
        Args:
            locations: List of locations to collect events from
            date_range: Date range to collect events for
            categories: Optional list of categories to filter by
            
        Returns:
            Total number of new/updated events
        """
        if not locations:
            locations = self.DEFAULT_LOCATIONS
            
        if not date_range:
            date_range = {
                'start': datetime.utcnow(),
                'end': datetime.utcnow() + timedelta(days=30)
            }
            
        total_events = 0
        tasks = []
        
        # Create tasks for each location and scraper combination
        for location in locations:
            if not self._validate_location(location):
                logger.error(f"Invalid location format: {location}")
                continue
                
            scraper_location = self._format_location(location)
            
            for scraper in self.scrapers:
                task = self._collect_from_scraper(
                    scraper=scraper,
                    location=scraper_location,
                    date_range=date_range,
                    categories=categories
                )
                tasks.append(task)
        
        # Run all collection tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and count total events
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error in collection task: {str(result)}")
            elif isinstance(result, int):
                total_events += result
                
        return total_events
        
    def _validate_location(self, location: Dict[str, Any]) -> bool:
        """Validate location data format"""
        required_fields = {'city', 'state', 'latitude', 'longitude', 'radius'}
        return all(field in location for field in required_fields)
        
    def _format_location(self, location: Dict[str, Any]) -> Dict[str, Any]:
        """Format location data for scrapers"""
        return {
            'city': location['city'],
            'state': location['state'],
            'lat': location['latitude'],
            'lng': location['longitude'],
            'radius': location['radius']
        }
        
    async def _collect_from_scraper(
        self,
        scraper: ScrapflyBaseScraper,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> int:
        """Collect events from a single scraper"""
        try:
            events = await scraper.scrape_events(location, date_range, categories)
            if not events:
                return 0
                
            self._store_events(events)
            return len(events)
            
        except Exception as e:
            logger.error(f"Error collecting events from {scraper.platform}: {str(e)}")
            return 0
            
    def _store_events(self, events: List[Dict[str, Any]]) -> None:
        """Store events in the database with proper error handling"""
        db = SessionLocal()
        try:
            stored_count = 0
            skipped_count = 0
            error_count = 0
            
            for event_data in events:
                try:
                    if not self._validate_event_data(event_data):
                        logger.warning(f"Invalid event data: {event_data.get('title', 'Unknown')}")
                        skipped_count += 1
                        continue
                        
                    # Check for existing event
                    existing = self._get_existing_event(db, event_data)
                    
                    if existing:
                        self._update_event(db, existing, event_data)
                    else:
                        self._create_event(db, event_data)
                        
                    stored_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing event: {str(e)}")
                    error_count += 1
                    
            db.commit()
            logger.info(f"Event storage summary: {stored_count} stored, {skipped_count} skipped, {error_count} errors")
            
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            db.rollback()
        finally:
            db.close()
            
    def _validate_event_data(self, event_data: Dict[str, Any]) -> bool:
        """Validate event data structure"""
        required_fields = {
            'title',
            'start_time',
            'location',
            'source',
            'url'
        }
        
        if not all(field in event_data for field in required_fields):
            return False
            
        # Validate nested fields
        if not isinstance(event_data['location'], dict):
            return False
            
        if not all(field in event_data['location'] for field in ['venue_name', 'address']):
            return False
            
        return True
        
    def _get_existing_event(self, db: Session, event_data: Dict[str, Any]) -> Optional[Event]:
        """Get existing event from database"""
        source = event_data['source']
        event_id = event_data.get('event_id')
        
        if not event_id:
            return None
            
        return db.query(Event).filter(
            Event.source == source,
            Event.source_id == event_id
        ).first()
        
    def _create_event(self, db: Session, event_data: Dict[str, Any]) -> Event:
        """Create a new event"""
        event = Event(
            title=event_data['title'],
            description=event_data.get('description', ''),
            start_time=event_data['start_time'],
            end_time=event_data.get('end_time'),
            location=event_data['location'],
            categories=event_data.get('categories', []),
            price_info=event_data.get('price_info', {}),
            source=event_data['source'],
            source_id=event_data.get('event_id'),
            url=event_data['url'],
            image_url=event_data.get('image_url'),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        db.add(event)
        return event
        
    def _update_event(self, db: Session, event: Event, event_data: Dict[str, Any]) -> None:
        """Update an existing event"""
        event.title = event_data['title']
        event.description = event_data.get('description', event.description)
        event.start_time = event_data['start_time']
        event.end_time = event_data.get('end_time', event.end_time)
        event.location = event_data['location']
        event.categories = event_data.get('categories', event.categories)
        event.price_info = event_data.get('price_info', event.price_info)
        event.url = event_data['url']
        event.image_url = event_data.get('image_url', event.image_url)
        event.updated_at = datetime.utcnow()
        
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
                
    async def stop_collection_task(self) -> None:
        """Stop the collection task"""
        self.running = False 