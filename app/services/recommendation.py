from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
from app.models.event import Event
from app.models.preferences import UserPreferences
from app.recommendation.engine import RecommendationEngine
from app.services.location_recommendations import LocationService
from app.services.price_normalization import PriceNormalizer
from app.services.category_extraction import CategoryExtractor

logger = logging.getLogger(__name__)

class RecommendationService:
    def __init__(self):
        """Initialize recommendation service with required components."""
        self.recommendation_engine = RecommendationEngine()
        self.location_service = LocationService()
        self.price_normalizer = PriceNormalizer()
        self.category_extractor = CategoryExtractor()

    async def get_personalized_recommendations(
        self,
        preferences: UserPreferences,
        events: List[Event],
        limit: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Event]:
        """
        Generate personalized event recommendations based on user preferences.
        
        Args:
            preferences: User preferences
            events: List of available events
            limit: Maximum number of recommendations to return
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of recommended events
        """
        try:
            # Filter events by date range if provided
            filtered_events = events
            if start_date:
                filtered_events = [e for e in filtered_events if e.start_time >= start_date]
            if end_date:
                filtered_events = [e for e in filtered_events if e.start_time <= end_date]

            # Get recommendations using the recommendation engine
            recommendations = self.recommendation_engine.get_personalized_recommendations(
                preferences=preferences,
                events=filtered_events,
                limit=limit
            )

            return recommendations

        except Exception as e:
            logger.error(f"Error generating personalized recommendations: {str(e)}")
            return []

    async def get_trending_recommendations(
        self,
        events: List[Event],
        timeframe_days: int = 7,
        limit: Optional[int] = None
    ) -> List[Event]:
        """
        Get trending events based on popularity and recency.
        
        Args:
            events: List of available events
            timeframe_days: Number of days to consider for trending
            limit: Maximum number of recommendations to return
            
        Returns:
            List of trending events
        """
        try:
            # Get trending recommendations using the recommendation engine
            trending = self.recommendation_engine.get_trending_recommendations(
                events=events,
                timeframe_days=timeframe_days,
                limit=limit
            )

            return trending

        except Exception as e:
            logger.error(f"Error getting trending recommendations: {str(e)}")
            return []

    async def get_similar_events(
        self,
        event: Event,
        events: List[Event],
        limit: Optional[int] = None
    ) -> List[Event]:
        """
        Find events similar to a given event.
        
        Args:
            event: Reference event
            events: List of events to search through
            limit: Maximum number of similar events to return
            
        Returns:
            List of similar events
        """
        try:
            # Create temporary preferences based on the reference event
            temp_preferences = UserPreferences(
                user_id="temp",
                preferred_categories=event.categories,
                excluded_categories=[],
                min_price=0.0,
                max_price=float('inf'),
                preferred_location=event.location,
                preferred_days=[],
                preferred_times=[]
            )

            # Get recommendations using the recommendation engine
            similar_events = self.recommendation_engine.get_personalized_recommendations(
                preferences=temp_preferences,
                events=[e for e in events if e.id != event.id],  # Exclude the reference event
                limit=limit
            )

            return similar_events

        except Exception as e:
            logger.error(f"Error finding similar events: {str(e)}")
            return []

    async def get_category_recommendations(
        self,
        category: str,
        events: List[Event],
        limit: Optional[int] = None
    ) -> List[Event]:
        """
        Get recommendations for a specific category.
        
        Args:
            category: Target category
            events: List of available events
            limit: Maximum number of recommendations to return
            
        Returns:
            List of recommended events in the category
        """
        try:
            # Create temporary preferences for the category
            temp_preferences = UserPreferences(
                user_id="temp",
                preferred_categories=[category],
                excluded_categories=[],
                min_price=0.0,
                max_price=float('inf'),
                preferred_location=None,
                preferred_days=[],
                preferred_times=[]
            )

            # Get recommendations using the recommendation engine
            category_events = self.recommendation_engine.get_personalized_recommendations(
                preferences=temp_preferences,
                events=events,
                limit=limit
            )

            return category_events

        except Exception as e:
            logger.error(f"Error getting category recommendations: {str(e)}")
            return [] 