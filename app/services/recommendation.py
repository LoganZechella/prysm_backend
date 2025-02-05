"""Service for generating event recommendations."""
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.event import EventModel
from app.models.preferences import UserPreferences, UserPreferencesBase, LocationPreference
from app.services.recommendation_engine import RecommendationEngine
from app.services.personalize_service import PersonalizeService
from app.services.interaction_tracking import InteractionTrackingService
from app.services.location_recommendations import LocationService
from app.services.price_normalization import PriceNormalizer
from app.services.category_extraction import CategoryExtractor
from app.services.cache_service import CacheService
from app.services.event_service import EventService
from app.utils.pagination import PaginationParams
from app.core.config import settings

logger = logging.getLogger(__name__)

class RecommendationService:
    def __init__(self, db: AsyncSession):
        """Initialize recommendation service with required components."""
        self.db = db
        # Keep the custom engine for fallback
        self.recommendation_engine = RecommendationEngine(db=db)
        # Add Personalize service
        self.personalize_service = PersonalizeService(db=db)
        # Add interaction tracking
        self.interaction_service = InteractionTrackingService(db=db)
        
        self.location_service = LocationService()
        self.price_normalizer = PriceNormalizer()
        self.category_extractor = CategoryExtractor()
        self.cache_service = CacheService()
        self.event_service = EventService()
        
    async def get_recommendations(
        self,
        user_id: str,
        preferences: Optional[Dict[str, Any]] = None,
        num_results: int = 25,
        use_personalize: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get recommendations for a user, using Amazon Personalize if available,
        falling back to custom engine if needed.
        """
        try:
            if use_personalize and settings.AWS_PERSONALIZE_CAMPAIGN_ARN:
                # Get recommendations from Personalize
                event_ids = await self.personalize_service.get_recommendations(
                    user_id=user_id,
                    num_results=num_results
                )
                
                if event_ids:
                    # Convert event IDs to full event data
                    events = []
                    for event_id in event_ids:
                        event = await self.event_service.get_event(int(event_id))
                        if event:
                            events.append(event.to_dict())
                    
                    # Apply any additional filters from preferences
                    filtered_events = await self._apply_preference_filters(
                        events=events,
                        preferences=preferences
                    )
                    
                    return filtered_events
                
            # Fall back to custom engine if Personalize fails or is not configured
            logger.info("Falling back to custom recommendation engine")
            return await self.recommendation_engine.get_recommendations(
                user_id=user_id,
                preferences=preferences,
                max_results=num_results
            )
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            # Fall back to custom engine on error
            return await self.recommendation_engine.get_recommendations(
                user_id=user_id,
                preferences=preferences,
                max_results=num_results
            )
    
    async def track_interaction(
        self,
        user_id: str,
        event_id: int,
        interaction_type: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Track user interaction with an event."""
        try:
            # Track in local database
            if interaction_type == 'view':
                await self.interaction_service.track_view(
                    user_id=user_id,
                    event_id=event_id,
                    context=context
                )
            elif interaction_type == 'click':
                await self.interaction_service.track_click(
                    user_id=user_id,
                    event_id=event_id,
                    context=context
                )
            elif interaction_type == 'rsvp':
                rsvp_status = context.get('rsvp_status', True) if context else True
                await self.interaction_service.track_rsvp(
                    user_id=user_id,
                    event_id=event_id,
                    rsvp_status=rsvp_status,
                    context=context
                )
            
            # Record in Personalize if available
            if settings.AWS_PERSONALIZE_TRACKING_ID:
                event_type = settings.PERSONALIZE_EVENT_TYPES.get(interaction_type)
                if event_type:
                    await self.personalize_service.record_event(
                        user_id=user_id,
                        event_id=event_id,
                        event_type=event_type,
                        event_value=1.0 if interaction_type == 'rsvp' else None
                    )
            
        except Exception as e:
            logger.error(f"Error tracking interaction: {str(e)}")
    
    async def _apply_preference_filters(
        self,
        events: List[Dict[str, Any]],
        preferences: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply user preferences as filters to recommended events."""
        if not preferences:
            return events
            
        filtered_events = []
        for event in events:
            # Apply category filter
            if preferred_categories := preferences.get('preferred_categories'):
                if not any(cat in event['categories'] for cat in preferred_categories):
                    continue
                    
            # Apply price filter
            if max_price := preferences.get('max_price'):
                event_price = event.get('price_info', {}).get('min_price', 0)
                if event_price and event_price > max_price:
                    continue
                    
            # Apply location filter
            if location := preferences.get('preferred_location'):
                if not self._is_within_distance(
                    event=event,
                    user_lat=location.get('latitude'),
                    user_lon=location.get('longitude'),
                    max_distance=preferences.get('max_distance', 50)
                ):
                    continue
                    
            filtered_events.append(event)
            
        return filtered_events
    
    def _is_within_distance(
        self,
        event: Dict[str, Any],
        user_lat: float,
        user_lon: float,
        max_distance: float
    ) -> bool:
        """Check if event is within max distance of user location."""
        if not all([user_lat, user_lon, event.get('venue_latitude'), event.get('venue_longitude')]):
            return True  # Include events with no location data
            
        distance = self.location_service.calculate_distance(
            lat1=user_lat,
            lon1=user_lon,
            lat2=event['venue_latitude'],
            lon2=event['venue_longitude']
        )
        
        return distance <= max_distance

    async def get_personalized_recommendations(
        self,
        preferences: UserPreferencesBase,
        pagination: Optional[PaginationParams] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Tuple[List[EventModel], Dict[str, Any]]:
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
            scored_events = await self.recommendation_engine.score_events_batch(events, preferences)
            
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
    ) -> Tuple[List[EventModel], Dict[str, Any]]:
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
            
            # Create temporary preferences for trending
            temp_preferences = UserPreferencesBase(
                user_id='trending',
                preferred_categories=[],
                excluded_categories=[],
                min_price=0.0,
                max_price=None,
                preferred_location=None,
                preferred_days=[],
                preferred_times=[]
            )
            
            # Generate cache key
            cache_key = f"trending:{timeframe_days}:{pagination.page}:{pagination.page_size}"
            
            # Try to get from cache with shorter TTL for trending
            cached = await self.cache_service.get_recommendations(
                'trending',
                temp_preferences,
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
                temp_preferences,
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
    ) -> List[EventModel]:
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
    ) -> Tuple[List[EventModel], Dict[str, Any]]:
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
            temp_preferences = UserPreferencesBase(
                user_id=f'category_{category}',
                preferred_categories=[category],
                excluded_categories=[],
                min_price=0.0,
                max_price=None,
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