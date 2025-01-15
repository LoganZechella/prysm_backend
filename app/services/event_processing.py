"""
Event processing service with validation and error handling.
"""

from typing import Dict, List, Any, Set, Optional
from datetime import datetime
import logging
from app.schemas.validation import ValidationPipeline, SchemaVersion, EventV2
from app.services.error_handling import with_retry, circuit_breaker
from app.services.nlp_service import NLPService
from app.services.location_recommendations import calculate_distance
from app.monitoring.performance import PerformanceMonitor

logger = logging.getLogger(__name__)

class EventProcessor:
    """Service for processing and enriching event data"""
    
    def __init__(self):
        self.validation_pipeline = ValidationPipeline()
        self.nlp_service = NLPService()
        self.performance_monitor = PerformanceMonitor()
    
    @with_retry(max_retries=3, circuit_breaker_name="event_processing")
    def process_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process and enrich event data
        
        Args:
            event_data: Raw event data
            
        Returns:
            Processed and enriched event data
        """
        try:
            with self.performance_monitor.monitor_api_call('process_event'):
                # Validate event data
                validated_data = self.validation_pipeline.validate_event(
                    event_data,
                    version=SchemaVersion.V2
                )
                
                # Clean text fields
                validated_data['title'] = validated_data['title'].strip()
                validated_data['description'] = " ".join(validated_data['description'].split())
                
                # Extract entities and sentiment
                nlp_results = self.nlp_service.analyze_text(
                    f"{validated_data['title']} {validated_data['description']}"
                )
                
                # Add NLP enrichments
                validated_data['metadata']['entities'] = nlp_results['entities']
                validated_data['metadata']['sentiment'] = nlp_results['sentiment']
                
                # Extract topics
                topics = self.extract_topics(validated_data['title'], validated_data['description'])
                if topics:
                    validated_data['tags'].extend(topics)
                    validated_data['tags'] = list(set(validated_data['tags']))  # Deduplicate
                
                return validated_data
                
        except Exception as e:
            self.performance_monitor.record_error('event_processing', str(e))
            raise
    
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
                    distance = calculate_distance(
                        event_coords,
                        user_preferences['preferred_location']
                    )
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