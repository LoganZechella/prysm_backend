from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging
from sqlalchemy.orm import Session
from app.models.event import Event
from app.models.preferences import UserPreferences
from app.recommendation.engine import RecommendationEngine
from app.services.location_recommendations import LocationService
from app.services.price_normalization import PriceNormalizer
from app.services.category_extraction import CategoryExtractor
from app.services.cache_service import CacheService
from app.services.event_service import EventService
from app.utils.pagination import PaginationParams

logger = logging.getLogger(__name__)

class RecommendationService:
    def __init__(self, db: Session):
        """Initialize recommendation service with required components."""
        self.db = db
        self.recommendation_engine = RecommendationEngine()
        self.location_service = LocationService()
        self.price_normalizer = PriceNormalizer()
        self.category_extractor = CategoryExtractor()
        self.cache_service = CacheService()
        self.event_service = EventService()

    async def get_personalized_recommendations(
        self,
        preferences: UserPreferences,
        pagination: Optional[PaginationParams] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Tuple[List[Event], Dict[str, Any]]:
        """
        Generate personalized event recommendations based on user preferences.
        
        Args:
            preferences: User preferences
            pagination: Optional pagination parameters
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Tuple of (recommended events, pagination info)
        """
        try:
            # Set default pagination if not provided
            if pagination is None:
                pagination = PaginationParams()
            
            # Generate cache key
            cache_key = f"recommendations:{preferences.user_id}:{pagination.page}:{pagination.page_size}"
            if start_date:
                cache_key += f":{start_date.isoformat()}"
            if end_date:
                cache_key += f":{end_date.isoformat()}"
            
            # Try to get from cache
            cached = await self.cache_service.get_recommendations(
                preferences.user_id,
                preferences,
                {
                    'pagination': pagination.dict(),
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                }
            )
            if cached:
                return cached, pagination.get_pagination_info(len(cached))

            # Get events using optimized service
            events, total_count = self.event_service.get_events_by_preferences(
                self.db,
                preferences,
                pagination,
                start_date,
                end_date
            )
            
            # Score and sort events using vectorized operations
            scored_events = self.recommendation_engine.score_events_batch(events, preferences)
            
            # Cache the results
            await self.cache_service.set_recommendations(
                preferences.user_id,
                preferences,
                events,
                {
                    'pagination': pagination.dict(),
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                }
            )

            return events, pagination.get_pagination_info(total_count)

        except Exception as e:
            logger.error(f"Error generating personalized recommendations: {str(e)}")
            return [], {'total_count': 0}

    async def get_trending_recommendations(
        self,
        timeframe_days: int = 7,
        pagination: Optional[PaginationParams] = None
    ) -> Tuple[List[Event], Dict[str, Any]]:
        """
        Get trending events based on popularity and recency.
        
        Args:
            timeframe_days: Number of days to consider for trending
            pagination: Optional pagination parameters
            
        Returns:
            Tuple of (trending events, pagination info)
        """
        try:
            # Set default pagination if not provided
            if pagination is None:
                pagination = PaginationParams()
            
            # Generate cache key
            cache_key = f"trending:{timeframe_days}:{pagination.page}:{pagination.page_size}"
            
            # Try to get from cache with shorter TTL for trending
            cached = await self.cache_service.get_recommendations(
                'trending',  # Special user_id for trending
                UserPreferences(user_id='trending'),  # Empty preferences
                {
                    'type': 'trending',
                    'timeframe_days': timeframe_days,
                    'pagination': pagination.dict()
                }
            )
            if cached:
                return cached, pagination.get_pagination_info(len(cached))

            # Get trending events using optimized service
            events, total_count = self.event_service.get_trending_events(
                self.db,
                timeframe_days,
                pagination
            )
            
            # Cache the results
            await self.cache_service.set_recommendations(
                'trending',
                UserPreferences(user_id='trending'),
                events,
                {
                    'type': 'trending',
                    'timeframe_days': timeframe_days,
                    'pagination': pagination.dict()
                }
            )

            return events, pagination.get_pagination_info(total_count)

        except Exception as e:
            logger.error(f"Error getting trending recommendations: {str(e)}")
            return [], {'total_count': 0}

    async def get_similar_events(
        self,
        event_id: int,
        limit: int = 10
    ) -> List[Event]:
        """
        Find events similar to a given event.
        
        Args:
            event_id: Reference event ID
            limit: Maximum number of similar events to return
            
        Returns:
            List of similar events
        """
        try:
            # Generate cache key
            cache_key = f"similar:{event_id}:{limit}"
            
            # Try to get from cache
            cached = await self.cache_service.get_recommendations(
                f'similar_{event_id}',  # Special user_id for similar events
                UserPreferences(user_id=f'similar_{event_id}'),  # Empty preferences
                {
                    'type': 'similar',
                    'reference_event': event_id,
                    'limit': limit
                }
            )
            if cached:
                return cached

            # Get similar events using optimized service
            similar_events = self.event_service.get_similar_events(
                self.db,
                event_id,
                limit
            )
            
            # Cache the results
            await self.cache_service.set_recommendations(
                f'similar_{event_id}',
                UserPreferences(user_id=f'similar_{event_id}'),
                similar_events,
                {
                    'type': 'similar',
                    'reference_event': event_id,
                    'limit': limit
                }
            )

            return similar_events

        except Exception as e:
            logger.error(f"Error finding similar events: {str(e)}")
            return []

    async def get_category_recommendations(
        self,
        category: str,
        pagination: Optional[PaginationParams] = None
    ) -> Tuple[List[Event], Dict[str, Any]]:
        """
        Get recommendations for a specific category.
        
        Args:
            category: Target category
            pagination: Optional pagination parameters
            
        Returns:
            Tuple of (recommended events, pagination info)
        """
        try:
            # Set default pagination if not provided
            if pagination is None:
                pagination = PaginationParams()
            
            # Create temporary preferences for the category
            temp_preferences = UserPreferences(
                user_id=f'category_{category}',
                preferred_categories=[category],
                excluded_categories=[],
                min_price=0.0,
                max_price=float('inf'),
                preferred_location=None,
                preferred_days=[],
                preferred_times=[]
            )
            
            # Get events using optimized service
            events, total_count = self.event_service.get_events_by_preferences(
                self.db,
                temp_preferences,
                pagination
            )
            
            # Cache the results
            await self.cache_service.set_recommendations(
                f'category_{category}',
                temp_preferences,
                events,
                {
                    'type': 'category',
                    'category': category,
                    'pagination': pagination.dict()
                }
            )

            return events, pagination.get_pagination_info(total_count)

        except Exception as e:
            logger.error(f"Error getting category recommendations: {str(e)}")
            return [], {'total_count': 0}

    async def invalidate_user_recommendations(self, user_id: str) -> None:
        """
        Invalidate all cached recommendations for a user.
        
        Args:
            user_id: User identifier
        """
        try:
            await self.cache_service.invalidate_recommendations(user_id)
        except Exception as e:
            logger.error(f"Error invalidating recommendations cache: {str(e)}")

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        try:
            return await self.cache_service.get_cache_stats()
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {} 