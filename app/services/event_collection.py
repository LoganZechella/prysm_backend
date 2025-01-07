from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncio
import logging
import os
from app.models.event import Event
from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper

logger = logging.getLogger(__name__)

class EventCollectionService:
    """Service to collect and store events from various sources"""
    
    def __init__(self):
        """Initialize the event collection service"""
        self.scrapers = []
        self._initialize_scrapers()
    
    def _initialize_scrapers(self):
        """Initialize available scrapers with API keys"""
        # Get Scrapfly API key
        scrapfly_key = os.getenv("SCRAPFLY_API_KEY")
        if not scrapfly_key:
            logger.error("SCRAPFLY_API_KEY environment variable not set. Event collection service cannot be initialized.")
            return

        # Define active scrapers with their platform names
        scraper_classes = [
            (EventbriteScrapflyScraper, "Eventbrite"),
            (MeetupScrapflyScraper, "Meetup"),
            (FacebookScrapflyScraper, "Facebook")
        ]
        
        # Initialize each scraper
        for scraper_class, platform in scraper_classes:
            try:
                scraper = scraper_class(scrapfly_key)
                scraper.initialize_scrapfly(scrapfly_key)
                self.scrapers.append(scraper)
                logger.info(f"Successfully initialized {platform} scraper")
            except Exception as e:
                logger.error(f"Failed to initialize {platform} scraper: {str(e)}")
                continue

        if not self.scrapers:
            logger.error("No scrapers were successfully initialized. Event collection service may not function properly.")
        else:
            logger.info(f"Event collection service initialized with {len(self.scrapers)} active scrapers")
    
    async def collect_events(
        self,
        locations: List[Dict[str, Any]],
        date_range: Dict[str, datetime]
    ) -> int:
        """Collect events from all sources for given locations and date range"""
        total_events = 0
        
        for location in locations:
            # Validate location format
            if not all(key in location for key in ['city', 'state', 'latitude', 'longitude']):
                logger.error(f"Invalid location format: {location}")
                continue
            
            # Format location for scrapers
            scraper_location = {
                'city': location['city'],
                'state': location['state'],
                'lat': location['latitude'],
                'lng': location['longitude']
            }
            
            # Collect events from each scraper
            for scraper in self.scrapers:
                try:
                    events = await scraper.scrape_events(scraper_location, date_range)
                    if events:
                        total_events += len(events)
                        await self._store_events(events)
                except Exception as e:
                    logger.error(f"Error collecting events from {scraper.platform}: {str(e)}")
                    continue
        
        return total_events
    
    async def _store_events(self, events: List[Dict[str, Any]]) -> None:
        """Store events in the database"""
        try:
            from app.db.session import SessionLocal
            db = SessionLocal()
            stored_count = 0
            skipped_count = 0
            error_count = 0
            
            try:
                for event_data in events:
                    try:
                        # Basic data validation before processing
                        if not event_data or not isinstance(event_data, dict):
                            logger.error("Invalid event data format")
                            error_count += 1
                            continue

                        # Log the raw event data for debugging
                        logger.debug(f"Processing event: {event_data.get('title', 'Unknown')} from {event_data.get('source', {}).get('platform', 'Unknown')}")

                        # Validate event data before storage
                        if not self._is_valid_event_data(event_data):
                            logger.warning(f"Skipping invalid event: {event_data.get('title', 'Unknown')} from {event_data.get('source', {}).get('platform', 'Unknown')}")
                            skipped_count += 1
                            continue

                        # Check if event already exists
                        platform = event_data.get('source', {}).get('platform')
                        event_id = event_data.get('event_id')
                        
                        if not platform or not event_id:
                            logger.error(f"Missing platform or event_id: {event_data.get('title', 'Unknown')}")
                            error_count += 1
                            continue

                        existing_event = db.query(Event).filter(
                            Event.source == platform,
                            Event.source_id == event_id
                        ).first()
                        
                        if existing_event:
                            # Update existing event
                            try:
                                self._update_event(existing_event, event_data)
                                logger.info(f"Updated existing event: {event_data['title']} ({platform}/{event_id})")
                            except Exception as e:
                                logger.error(f"Error updating event {event_id}: {str(e)}")
                                error_count += 1
                                continue
                        else:
                            # Create new event
                            try:
                                new_event = self._create_event(event_data)
                                db.add(new_event)
                                logger.info(f"Created new event: {event_data['title']} ({platform}/{event_id})")
                            except Exception as e:
                                logger.error(f"Error creating event {event_id}: {str(e)}")
                                error_count += 1
                                continue
                        
                        stored_count += 1

                    except Exception as e:
                        logger.error(f"Error processing event: {str(e)}")
                        error_count += 1
                        continue

                # Commit all changes at once
                db.commit()
                logger.info(f"Event storage summary: {stored_count} stored, {skipped_count} skipped, {error_count} errors")
                
            except Exception as e:
                logger.error(f"Error during event storage transaction: {str(e)}")
                db.rollback()
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Error initializing database session: {str(e)}")

    def _create_event(self, event_data: Dict[str, Any]) -> Event:
        """Create a new event from event data"""
        try:
            # Handle datetime fields
            start_time = event_data['start_datetime']
            if isinstance(start_time, str):
                try:
                    # Handle different datetime formats
                    if 'Z' in start_time:
                        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    else:
                        start_time = datetime.fromisoformat(start_time)
                except ValueError as e:
                    logger.error(f"Invalid start_datetime format: {start_time}")
                    raise
            
            end_time = event_data.get('end_datetime')
            if isinstance(end_time, str):
                try:
                    if 'Z' in end_time:
                        end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                    else:
                        end_time = datetime.fromisoformat(end_time)
                except ValueError:
                    logger.warning(f"Invalid end_datetime format: {end_time}, setting to None")
                    end_time = None

            # Extract and validate venue data
            venue = {
                'name': str(event_data['location']['venue_name']).strip(),
                'address': str(event_data['location']['address']).strip()
            }
            
            # Add coordinates if they exist and are valid
            lat = event_data['location'].get('lat')
            lng = event_data['location'].get('lng')
            if lat is not None and lng is not None:
                try:
                    lat = float(lat)
                    lng = float(lng)
                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                        venue['latitude'] = lat
                        venue['longitude'] = lng
                except (ValueError, TypeError):
                    logger.warning(f"Invalid coordinates for event {event_data.get('title')}: lat={lat}, lng={lng}")

            # Clean and validate other fields
            title = str(event_data['title']).strip()
            description = str(event_data.get('description', '')).strip()
            categories = [str(cat).strip() for cat in event_data.get('categories', []) if cat]
            tags = [str(tag).strip() for tag in event_data.get('tags', []) if tag]
            
            # Validate price info
            price_info = event_data.get('price_info', {})
            if isinstance(price_info, dict):
                try:
                    if 'min' in price_info:
                        price_info['min'] = float(price_info['min'])
                    if 'max' in price_info:
                        price_info['max'] = float(price_info['max'])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid price info for event {title}, using empty dict")
                    price_info = {}
            else:
                price_info = {}

            # Create the event object with cleaned data
            return Event(
                title=title,
                description=description,
                start_time=start_time,
                end_time=end_time,
                location=event_data['location'],
                categories=categories,
                price_info=price_info,
                source=event_data['source']['platform'],
                source_id=event_data['event_id'],
                url=event_data['source']['url'],
                image_url=event_data.get('images', [None])[0],
                venue=venue,
                tags=tags,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        except Exception as e:
            logger.error(f"Error creating event object: {str(e)}")
            raise

    def _update_event(self, event: Event, event_data: Dict[str, Any]) -> None:
        """Update an existing event with new data"""
        try:
            # Handle datetime fields
            start_time = event_data['start_datetime']
            if isinstance(start_time, str):
                try:
                    if 'Z' in start_time:
                        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    else:
                        start_time = datetime.fromisoformat(start_time)
                except ValueError as e:
                    logger.error(f"Invalid start_datetime format: {start_time}")
                    raise
            
            end_time = event_data.get('end_datetime')
            if isinstance(end_time, str):
                try:
                    if 'Z' in end_time:
                        end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                    else:
                        end_time = datetime.fromisoformat(end_time)
                except ValueError:
                    logger.warning(f"Invalid end_datetime format: {end_time}, setting to None")
                    end_time = None

            # Extract and validate venue data
            venue = {
                'name': str(event_data['location']['venue_name']).strip(),
                'address': str(event_data['location']['address']).strip()
            }
            
            # Add coordinates if they exist and are valid
            lat = event_data['location'].get('lat')
            lng = event_data['location'].get('lng')
            if lat is not None and lng is not None:
                try:
                    lat = float(lat)
                    lng = float(lng)
                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                        venue['latitude'] = lat
                        venue['longitude'] = lng
                except (ValueError, TypeError):
                    logger.warning(f"Invalid coordinates for event {event_data.get('title')}: lat={lat}, lng={lng}")

            # Clean and validate other fields
            title = str(event_data['title']).strip()
            description = str(event_data.get('description', '')).strip()
            categories = [str(cat).strip() for cat in event_data.get('categories', []) if cat]
            tags = [str(tag).strip() for tag in event_data.get('tags', []) if tag]
            
            # Validate price info
            price_info = event_data.get('price_info', {})
            if isinstance(price_info, dict):
                try:
                    if 'min' in price_info:
                        price_info['min'] = float(price_info['min'])
                    if 'max' in price_info:
                        price_info['max'] = float(price_info['max'])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid price info for event {title}, using empty dict")
                    price_info = {}
            else:
                price_info = {}

            # Update event fields with cleaned data
            event.title = title
            event.description = description
            event.start_time = start_time
            event.end_time = end_time
            event.location = event_data['location']
            event.categories = categories
            event.price_info = price_info
            event.url = event_data['source']['url']
            event.image_url = event_data.get('images', [None])[0]
            event.venue = venue
            event.tags = tags
            event.updated_at = datetime.utcnow()
        except Exception as e:
            logger.error(f"Error updating event object: {str(e)}")
            raise

    def _is_valid_event_data(self, event_data: Dict[str, Any]) -> bool:
        """Validate event data before storing"""
        required_fields = {
            'title': str,
            'description': str,
            'start_datetime': datetime,
            'event_id': str,
            'source': {
                'platform': str,
                'url': str
            },
            'location': {
                'venue_name': str,
                'address': str
            }
        }
        
        try:
            # Check all required fields exist and have correct types
            for field, field_type in required_fields.items():
                if field not in event_data:
                    logger.warning(f"Missing required field: {field}")
                    return False
                    
                if field == 'source' or field == 'location':
                    # Check nested fields
                    for nested_field, nested_type in field_type.items():
                        if nested_field not in event_data[field]:
                            logger.warning(f"Missing required nested field: {field}.{nested_field}")
                            return False
                        if not isinstance(event_data[field][nested_field], nested_type):
                            logger.warning(f"Invalid type for {field}.{nested_field}")
                            return False
                else:
                    # Handle datetime field separately
                    if field == 'start_datetime':
                        if not isinstance(event_data[field], (datetime, str)):
                            logger.warning("Invalid start_datetime type")
                            return False
                    else:
                        if not isinstance(event_data[field], field_type):
                            logger.warning(f"Invalid type for {field}")
                            return False
            
            # Additional validation
            if not event_data['title'].strip():
                logger.warning("Empty title")
                return False
                
            if not event_data['location']['venue_name'].strip() or not event_data['location']['address'].strip():
                logger.warning("Empty venue name or address")
                return False
                
            if not event_data['source']['url'].startswith(('http://', 'https://')):
                logger.warning("Invalid URL format")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating event data: {str(e)}")
            return False
            
    def _update_event(self, event: Event, event_data: Dict[str, Any]) -> None:
        """Update an existing event with new data"""
        event.title = event_data['title']
        event.description = event_data['description']
        event.start_time = event_data['start_datetime']
        event.end_time = event_data.get('end_datetime')
        event.location = event_data['location']
        event.categories = event_data.get('categories', [])
        event.price_info = event_data.get('price_info', {})
        event.url = event_data['source']['url']
        event.image_url = event_data.get('images', [None])[0]
        event.venue = {
            'name': event_data['location']['venue_name'],
            'address': event_data['location']['address']
        }
        event.tags = event_data.get('tags', [])
        event.updated_at = datetime.utcnow()
        
    def _create_event(self, event_data: Dict[str, Any]) -> Event:
        """Create a new event from event data"""
        return Event(
            title=event_data['title'],
            description=event_data['description'],
            start_time=event_data['start_datetime'],
            end_time=event_data.get('end_datetime'),
            location=event_data['location'],
            categories=event_data.get('categories', []),
            price_info=event_data.get('price_info', {}),
            source=event_data['source']['platform'],
            source_id=event_data['event_id'],
            url=event_data['source']['url'],
            image_url=event_data.get('images', [None])[0],
            venue={
                'name': event_data['location']['venue_name'],
                'address': event_data['location']['address']
            },
            tags=event_data.get('tags', [])
        ) 