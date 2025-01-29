"""
Event processing service with validation and error handling.
"""

from typing import Dict, List, Any, Set, Optional
from datetime import datetime
import logging
from sqlalchemy.orm import Session
from app.schemas.validation import ValidationPipeline, SchemaVersion, EventV2
from app.services.error_handling import with_retry, circuit_breaker
from app.services.nlp_service import NLPService
from app.services.location_recommendations import LocationService
from app.monitoring.performance import PerformanceMonitor
from app.models.event import EventModel
from app.services.price_normalization import normalize_price
from app.services.category_hierarchy import get_category_hierarchy

logger = logging.getLogger(__name__)

class EventProcessor:
    """Service for processing and normalizing event data."""
    
    def __init__(self, db: Session):
        """Initialize event processor."""
        self.db = db
        self.validation_pipeline = ValidationPipeline()
        self.nlp_service = NLPService()
        self.performance_monitor = PerformanceMonitor()
        self.location_service = LocationService()
    
    @with_retry(max_retries=3, circuit_breaker_name="event_processing")
    def process_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process and normalize raw event data.
        
        Args:
            event_data: Raw event data from scraper
            
        Returns:
            Processed event data or None if invalid
        """
        try:
            processed = {}
            
            # Basic validation
            required_fields = ["title", "start_datetime"]
            if not all(field in event_data for field in required_fields):
                logger.warning(f"Missing required fields in event: {event_data.get('title', 'Unknown')}")
                return None
                
            # Copy basic fields
            processed.update({
                "title": event_data["title"],
                "description": event_data.get("description"),
                "start_datetime": event_data["start_datetime"],
                "end_datetime": event_data.get("end_datetime"),
                "url": event_data.get("url"),
                "platform": event_data.get("platform"),
                "platform_id": event_data.get("platform_id"),
                "is_online": event_data.get("is_online", False),
                "rsvp_count": event_data.get("rsvp_count", 0),
                "categories": event_data.get("categories", []),
                "image_url": event_data.get("image_url")
            })
            
            # Process venue information
            venue = event_data.get("venue", {})
            if isinstance(venue, str):
                venue = {"name": venue}
                
            # Get venue coordinates if not provided
            if not (venue.get("lat") or venue.get("latitude")):
                coords = self.location_service.geocode_venue(
                    venue_name=venue.get("name", ""),
                    city=venue.get("city", ""),
                    state=venue.get("state", ""),
                    country=venue.get("country", "US")
                )
                if coords:
                    venue.update(coords)
                    
            processed.update({
                "venue_name": venue.get("name"),
                "venue_lat": venue.get("lat") or venue.get("latitude"),
                "venue_lon": venue.get("lon") or venue.get("lng") or venue.get("longitude"),
                "venue_city": venue.get("city"),
                "venue_state": venue.get("state"),
                "venue_country": venue.get("country", "US")
            })
            
            # Process organizer information
            organizer = event_data.get("organizer", {})
            if isinstance(organizer, str):
                organizer = {"name": organizer}
                
            processed.update({
                "organizer_id": organizer.get("id"),
                "organizer_name": organizer.get("name")
            })
            
            # Process price information
            price_info = event_data.get("price_info") or event_data.get("prices", [])
            if price_info:
                processed["price_info"] = normalize_price(price_info)
            
            # Validate coordinates
            if processed.get("venue_lat") and processed.get("venue_lon"):
                try:
                    processed["venue_lat"] = float(processed["venue_lat"])
                    processed["venue_lon"] = float(processed["venue_lon"])
                except (ValueError, TypeError):
                    logger.warning(f"Invalid coordinates for event: {processed['title']}")
                    processed["venue_lat"] = None
                    processed["venue_lon"] = None
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}")
            return None
            
    def deduplicate_events(
        self,
        events: List[Dict[str, Any]],
        similarity_threshold: float = 0.8
    ) -> List[Dict[str, Any]]:
        """
        Remove duplicate events based on title and location similarity.
        
        Args:
            events: List of events to deduplicate
            similarity_threshold: Minimum similarity score to consider events duplicates
            
        Returns:
            Deduplicated list of events
        """
        try:
            unique_events = []
            
            for event in events:
                is_duplicate = False
                
                for unique_event in unique_events:
                    # Check title similarity
                    if self._calculate_title_similarity(
                        event["title"],
                        unique_event["title"]
                    ) >= similarity_threshold:
                        # If titles are similar, check location
                        if self._are_locations_same(event, unique_event):
                            is_duplicate = True
                            break
                
                if not is_duplicate:
                    unique_events.append(event)
            
            return unique_events
            
        except Exception as e:
            logger.error(f"Error deduplicating events: {str(e)}")
            return events
            
    def _calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity score between two event titles."""
        from difflib import SequenceMatcher
        return SequenceMatcher(None, title1.lower(), title2.lower()).ratio()
        
    def _are_locations_same(self, event1: Dict[str, Any], event2: Dict[str, Any]) -> bool:
        """Check if two events are at the same location."""
        try:
            # Get coordinates for both events
            coords1 = {
                "lat": event1.get("venue_lat"),
                "lon": event1.get("venue_lon")
            }
            
            coords2 = {
                "lat": event2.get("venue_lat"),
                "lon": event2.get("venue_lon")
            }
            
            # If both events have coordinates, check distance
            if all(coords1.values()) and all(coords2.values()):
                distance = self.location_service.calculate_distance(coords1, coords2)
                return distance < 0.1  # Less than 100 meters apart
                
            # If no coordinates, check venue names and cities
            return (
                event1.get("venue_name") == event2.get("venue_name") and
                event1.get("venue_city") == event2.get("venue_city") and
                event1.get("venue_state") == event2.get("venue_state")
            )
            
        except Exception as e:
            logger.error(f"Error comparing event locations: {str(e)}")
            return False
    
    def extract_topics(self, title: str, description: str) -> Set[str]:
        """
        Extract topics from event text
        
        Args:
            title: Event title
            description: Event description
            
        Returns:
            Set of extracted topics
        """
        try:
            with self.performance_monitor.monitor_api_call('extract_topics'):
                # Use NLP service to extract entities
                nlp_results = self.nlp_service.analyze_text(f"{title} {description}")
                
                # Extract topics from entities
                topics = set()
                for entity in nlp_results['entities']:
                    if entity['type'] in {'OTHER', 'EVENT', 'WORK_OF_ART'}:
                        topics.add(entity['name'].lower())
                
                # Filter topics against known list
                valid_topics = {
                    "music", "jazz", "blues", "rock", "classical",
                    "sports", "arts", "theater", "dance", "comedy",
                    "food", "drink", "festival", "conference",
                    "workshop", "seminar", "concert", "performance"
                }
                
                return topics.intersection(valid_topics)
                
        except Exception as e:
            logger.warning(f"Topic extraction failed: {str(e)}")
            return set()
    
    @with_retry(max_retries=2, circuit_breaker_name="event_scoring")
    def calculate_event_scores(
        self,
        event: Dict[str, Any],
        user_preferences: Dict[str, Any]
    ) -> Dict[str, float]:
        """
        Calculate various scores for an event based on user preferences
        
        Args:
            event: Validated event data
            user_preferences: User preferences
            
        Returns:
            Dictionary of scores
        """
        try:
            with self.performance_monitor.monitor_api_call('calculate_scores'):
                scores = {}
                
                # Calculate category score
                event_categories = {cat['name'].lower() for cat in event['categories']}
                preferred_categories = {
                    cat.lower() for cat in user_preferences.get("preferred_categories", [])
                }
                excluded_categories = {
                    cat.lower() for cat in user_preferences.get("excluded_categories", [])
                }
                
                if excluded_categories & event_categories:
                    scores["category_score"] = 0.0
                else:
                    matching_categories = preferred_categories & event_categories
                    scores["category_score"] = len(matching_categories) / max(len(preferred_categories), 1)
                
                # Calculate price score
                if event.get('prices'):
                    min_price = user_preferences.get("min_price", 0)
                    max_price = user_preferences.get("max_price", float("inf"))
                    event_price = max(price['amount'] for price in event['prices'])
                    
                    if min_price <= event_price <= max_price:
                        scores["price_score"] = 1.0
                    else:
                        price_range = max_price - min_price
                        if price_range > 0:
                            distance_from_range = min(
                                abs(event_price - min_price),
                                abs(event_price - max_price)
                            )
                            scores["price_score"] = max(0, 1 - (distance_from_range / price_range))
                        else:
                            scores["price_score"] = 0.0
                else:
                    scores["price_score"] = 1.0  # Free events get maximum score
                
                # Calculate distance score
                if (event.get('location') and
                    event['location'].get('latitude') is not None and
                    event['location'].get('longitude') is not None and
                    user_preferences.get('preferred_location')):
                    
                    event_coords = {
                        'latitude': event['location']['latitude'],
                        'longitude': event['location']['longitude']
                    }
                    distance = self.location_service.calculate_distance(event_coords, user_preferences['preferred_location'])
                    max_distance = user_preferences.get("max_distance", 50)  # Default 50km
                    scores["distance_score"] = max(0, 1 - (distance / max_distance))
                else:
                    scores["distance_score"] = 0.5  # Neutral score if location info missing
                
                # Calculate sentiment score from NLP analysis
                if event.get('metadata', {}).get('sentiment'):
                    sentiment = event['metadata']['sentiment']['document_sentiment']
                    scores["sentiment_score"] = (sentiment['score'] + 1) / 2  # Normalize to 0-1
                else:
                    scores["sentiment_score"] = 0.5
                
                # Calculate total score with weights
                weights = {
                    "category_score": 0.35,
                    "price_score": 0.20,
                    "distance_score": 0.25,
                    "sentiment_score": 0.20
                }
                
                scores["total_score"] = sum(
                    score * weights[name]
                    for name, score in scores.items()
                    if name != "total_score"
                )
                
                return scores
                
        except Exception as e:
            self.performance_monitor.record_error('event_scoring', str(e))
            raise 