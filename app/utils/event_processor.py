import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from app.utils.schema import Event, UserPreferences
from app.utils.recommendation import RecommendationEngine
from app.utils.event_processing import enrich_event

logger = logging.getLogger(__name__)

class EventProcessor:
    """Handles event processing and recommendation pipeline"""
    
    def __init__(self):
        """Initialize the event processor"""
        self.recommendation_engine = RecommendationEngine()
    
    async def process_and_recommend_events(
        self,
        raw_events: List[Dict[str, Any]],
        user_preferences: Optional[UserPreferences] = None,
        num_recommendations: int = 10
    ) -> List[Event]:
        """Process raw events and generate recommendations"""
        try:
            # Process and enrich events
            processed_events = []
            for raw_event in raw_events:
                try:
                    # Create event from raw data
                    event = Event.from_raw_data(raw_event)
                    
                    # Enrich event with additional data
                    enriched_event = await enrich_event(event)
                    processed_events.append(enriched_event)
                    
                except Exception as e:
                    logger.error(f"Error processing event: {str(e)}")
                    continue
            
            # If user preferences provided, generate personalized recommendations
            if user_preferences:
                recommendations = self.recommendation_engine.get_personalized_recommendations(
                    user_preferences,
                    processed_events,
                    n=num_recommendations
                )
                return [event for event, _ in recommendations]
            
            return processed_events
            
        except Exception as e:
            logger.error(f"Error in process_and_recommend_events: {str(e)}")
            return []
    
    def get_similar_events(
        self,
        event: Event,
        candidate_events: List[Event],
        num_similar: int = 5
    ) -> List[Event]:
        """Get similar events to a given event"""
        try:
            similar_events = self.recommendation_engine.get_similar_events(
                event,
                candidate_events,
                n=num_similar
            )
            return [event for event, _ in similar_events]
            
        except Exception as e:
            logger.error(f"Error getting similar events: {str(e)}")
            return []
    
    def filter_events_by_preferences(
        self,
        events: List[Event],
        preferences: UserPreferences,
        min_score: float = 0.5
    ) -> List[Event]:
        """Filter events based on user preferences"""
        try:
            filtered_events = []
            for event in events:
                score = self.recommendation_engine._calculate_preference_score(event, preferences)
                if score >= min_score:
                    filtered_events.append(event)
            return filtered_events
            
        except Exception as e:
            logger.error(f"Error filtering events: {str(e)}")
            return [] 