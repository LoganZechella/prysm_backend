"""Recommendation engine service for event recommendations."""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, text
from sqlalchemy.sql import Select
from app.models.event import EventModel
from app.models.traits import Traits
from app.models.preferences import UserPreferences, UserPreferencesBase, LocationPreference
from app.services.location_recommendations import LocationService
from app.utils.deduplication import calculate_title_similarity
from app.utils.logging import setup_logger
import json

logger = setup_logger(__name__)

class RecommendationEngine:
    """Engine for generating personalized event recommendations."""
    
    def __init__(self, db: AsyncSession):
        """Initialize recommendation engine."""
        self.db = db
        self.default_max_distance = 50.0  # km
        self.default_max_results = 50
        self.default_min_similarity = 0.8
        self.location_service = LocationService()
        
    async def get_recommendations(
        self,
        user_id: str,
        user_location: Optional[Dict[str, Any]] = None,
        preferences: Optional[Dict[str, Any]] = None,
        max_results: int = 20
    ) -> List[Dict[str, Any]]:
        """Get recommendations for a user."""
        try:
            # Get user traits
            traits = await self.db.execute(
                select(Traits).filter_by(user_id=user_id)
            )
            traits = traits.scalar_one_or_none()
            
            # Get candidate events
            events = await self._get_candidate_events(
                categories=preferences.get("categories", []) if preferences else [],
                location=user_location,
                max_distance=preferences.get("max_distance", 50) if preferences else 50,
                max_price=preferences.get("max_price") if preferences else None
            )
            
            if not events:
                logger.warning("No candidate events found")
                return []
                
            # Score events
            scored_events = []
            for event in events:
                try:
                    score = await self._score_event(event, preferences)
                    scored_events.append({
                        "id": str(event.id),
                        "title": event.title,
                        "description": event.description,
                        "start_datetime": event.start_datetime,
                        "end_datetime": event.end_datetime,
                        "venue_name": event.venue_name,
                        "venue_city": event.venue_city,
                        "venue_state": event.venue_state,
                        "venue_country": event.venue_country,
                        "venue_latitude": event.venue_latitude,
                        "venue_longitude": event.venue_longitude,
                        "categories": event.categories,
                        "url": event.url,
                        "relevance_score": score
                    })
                except Exception as e:
                    logger.error(f"Error scoring event {event.id}: {str(e)}")
                    continue
                    
            # Sort by score
            scored_events.sort(key=lambda x: x["relevance_score"], reverse=True)
            
            # Return top N results
            return scored_events[:max_results]
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            return []
            
    async def _get_candidate_events(
        self,
        categories: List[str],
        location: Optional[Dict[str, Any]],
        max_distance: float = 50,  # km
        max_price: Optional[float] = None
    ) -> List[EventModel]:
        """Get candidate events based on basic criteria."""
        try:
            # Start with base query
            query = select(EventModel).where(
                EventModel.start_datetime >= datetime.utcnow()
            )
            
            # Filter by location if provided
            if location:
                lat = float(location.get("latitude", 0))
                lon = float(location.get("longitude", 0))
                
                # Add location-based filtering
                query = query.where(
                    and_(
                        # Allow events with null coordinates or within distance
                        or_(
                            and_(
                                EventModel.venue_lat.isnot(None),
                                EventModel.venue_lon.isnot(None),
                                # Use Haversine formula to calculate distance
                                func.acos(
                                    func.sin(func.radians(lat)) * func.sin(func.radians(EventModel.venue_lat)) +
                                    func.cos(func.radians(lat)) * func.cos(func.radians(EventModel.venue_lat)) *
                                    func.cos(func.radians(EventModel.venue_lon) - func.radians(lon))
                                ) * 6371 <= max_distance  # 6371 is Earth's radius in km
                            ),
                            and_(
                                EventModel.venue_lat.is_(None),
                                EventModel.venue_lon.is_(None)
                            )
                        )
                    )
                )
                
            # Filter by categories if provided
            if categories:
                query = query.where(
                    EventModel.categories.overlap(categories)
                )
                
            # Filter by price if provided
            if max_price is not None:
                query = query.where(
                    or_(
                        EventModel.price.is_(None),
                        EventModel.price <= max_price
                    )
                )
                
            # Execute query
            result = await self.db.execute(query)
            events = result.scalars().all()
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting candidate events: {str(e)}")
            return []
            
    async def _score_event(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate a score for an event based on user preferences."""
        try:
            # Calculate individual scores
            location_score = await self._calculate_location_score(event, preferences)
            category_score = self._calculate_category_score(event, preferences)
            price_score = self._calculate_price_score(event, preferences)
            time_score = self._calculate_time_score(event, preferences)
            
            # Combine scores with weights
            weights = {
                'location': 0.4,  # Location is most important
                'category': 0.3,  # Categories are next
                'price': 0.2,    # Price is moderately important
                'time': 0.1      # Time is least important
            }
            
            total_score = (
                weights['location'] * location_score +
                weights['category'] * category_score +
                weights['price'] * price_score +
                weights['time'] * time_score
            )
            
            return total_score
            
        except Exception as e:
            logger.error(f"Error scoring event {event.id}: {str(e)}")
            return 0.0
            
    def _calculate_category_score(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate category preference score."""
        try:
            if isinstance(preferences, dict):
                preferred_categories = preferences.get("preferred_categories", [])
                excluded_categories = preferences.get("excluded_categories", [])
            else:
                preferred_categories = preferences.preferred_categories or []
                excluded_categories = preferences.excluded_categories or []
            
            # If no categories specified, return neutral score
            if not preferred_categories:
                return 0.5
            
            # If event has any excluded categories, return 0
            if any(cat in excluded_categories for cat in event.categories):
                return 0.0
            
            # Calculate match percentage with preferred categories
            matches = sum(1 for cat in event.categories if cat in preferred_categories)
            if not matches:
                return 0.1  # Small non-zero score for events with no matching categories
            
            return min(1.0, matches / len(preferred_categories))
            
        except Exception as e:
            logger.error(f"Error calculating category score: {str(e)}")
            return 0.0
            
    def _calculate_price_score(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate price preference score."""
        try:
            if isinstance(preferences, dict):
                max_price = preferences.get("max_price")
                if max_price is None:
                    return 1.0
            else:
                max_price = preferences.max_price
                if max_price is None:
                    return 1.0
            
            # If event is free, return perfect score
            if not event.price_info or event.price_info == "null" or event.price_info == []:
                return 1.0
            
            # Extract price from event
            try:
                if isinstance(event.price_info, str):
                    price_info = json.loads(event.price_info)
                else:
                    price_info = event.price_info
                
                if isinstance(price_info, list):
                    if not price_info:
                        return 1.0
                    # Use minimum price from list
                    price = min(float(p.get("amount", 0)) for p in price_info)
                else:
                    price = float(price_info.get("amount", 0))
            except (ValueError, AttributeError, json.JSONDecodeError):
                return 1.0
            
            # Score based on price relative to max price
            if price <= 0:
                return 1.0
            elif price >= max_price:
                return 0.0
            else:
                return 1.0 - (price / max_price)
            
        except Exception as e:
            logger.error(f"Error calculating price score: {str(e)}")
            return 0.0
            
    def _calculate_time_score(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate time preference score."""
        try:
            if isinstance(preferences, dict):
                preferred_days = preferences.get("preferred_days", [])
                preferred_times = preferences.get("preferred_times", [])
            else:
                preferred_days = preferences.preferred_days or []
                preferred_times = preferences.preferred_times or []
            
            # If no time preferences, return neutral score
            if not (preferred_days or preferred_times):
                return 0.5
            
            scores = []
            
            # Score day match
            if preferred_days:
                event_day = event.start_datetime.strftime("%A").lower()
                day_match = event_day in [day.lower() for day in preferred_days]
                scores.append(1.0 if day_match else 0.0)
            
            # Score time match
            if preferred_times:
                event_hour = event.start_datetime.hour
                time_ranges = {
                    "morning": (5, 12),
                    "afternoon": (12, 17),
                    "evening": (17, 22),
                    "night": (22, 5)
                }
                
                # Check if event time falls within any preferred time range
                time_match = any(
                    start <= event_hour < end if start < end else
                    (event_hour >= start or event_hour < end)
                    for time_range in preferred_times
                    for start, end in [time_ranges[time_range.lower()]]
                    if time_range.lower() in time_ranges
                )
                scores.append(1.0 if time_match else 0.0)
            
            # Average all scores
            return sum(scores) / len(scores) if scores else 0.5
            
        except Exception as e:
            logger.error(f"Error calculating time score: {str(e)}")
            return 0.0
            
    async def _calculate_location_score(
        self,
        event: EventModel,
        preferences: Union[UserPreferencesBase, Dict[str, Any]]
    ) -> float:
        """Calculate location preference score."""
        try:
            if isinstance(preferences, dict):
                if not preferences.get("preferred_location"):
                    return 1.0
                user_lat = float(preferences["preferred_location"].get("latitude", 0))
                user_lon = float(preferences["preferred_location"].get("longitude", 0))
                max_distance = preferences.get("max_distance", self.default_max_distance)
            else:
                if not preferences.preferred_location:
                    return 1.0
                user_lat = float(preferences.preferred_location.latitude or 0)
                user_lon = float(preferences.preferred_location.longitude or 0)
                max_distance = preferences.max_distance or self.default_max_distance
            
            if not (event.venue_lat and event.venue_lon):
                return 0.0
            
            # Calculate distance
            distance = self._calculate_distance(
                user_lat,
                user_lon,
                event.venue_lat,
                event.venue_lon
            )
            
            # Score based on distance
            return max(0.0, 1.0 - (distance / max_distance))
            
        except Exception as e:
            logger.error(f"Error calculating location score: {str(e)}")
            return 0.0
    
    def _calculate_distance(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float
    ) -> float:
        """Calculate distance between two points in kilometers."""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371  # Earth's radius in kilometers
        
        lat1, lon1 = radians(lat1), radians(lon1)
        lat2, lon2 = radians(lat2), radians(lon2)
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        
        return R * c
    
    async def score_events_batch(
        self,
        events: List[EventModel],
        preferences: UserPreferencesBase
    ) -> List[Dict[str, Any]]:
        """
        Score a batch of events based on user preferences.
        
        Args:
            events: List of events to score
            preferences: User preferences
            
        Returns:
            List of scored events with metadata
        """
        try:
            scored_events = []
            
            for event in events:
                # Calculate category match score (30%)
                category_score = 0.0
                if preferences.preferred_categories:
                    matching_categories = len(
                        set(event.categories) & set(preferences.preferred_categories)
                    )
                    category_score = matching_categories / max(len(event.categories), 1)
                
                # Calculate price score (20%)
                price_score = 1.0  # Default to best score for free events
                if event.price_info and preferences.max_price:
                    event_price = float(event.price_info.get("max", 0))
                    if event_price > preferences.max_price:
                        price_score = max(0.0, 1.0 - (event_price - preferences.max_price) / preferences.max_price)
                
                # Calculate location score (30%)
                location_score = 1.0  # Default to best score if no location preference
                if preferences.preferred_location and event.venue_lat and event.venue_lon:
                    distance = self._calculate_distance(
                        preferences.preferred_location.latitude,
                        preferences.preferred_location.longitude,
                        event.venue_lat,
                        event.venue_lon
                    )
                    max_distance = preferences.max_distance or self.default_max_distance
                    location_score = max(0.0, 1.0 - (distance / max_distance))
                
                # Calculate time score (20%)
                time_score = await self._calculate_time_score(event, {})
                
                # Calculate final score
                final_score = (
                    0.3 * category_score +
                    0.2 * price_score +
                    0.3 * location_score +
                    0.2 * time_score
                )
                
                scored_events.append({
                    "event": event,
                    "score": final_score,
                    "metadata": {
                        "category_score": category_score,
                        "price_score": price_score,
                        "location_score": location_score,
                        "time_score": time_score
                    }
                })
            
            # Sort by score
            scored_events.sort(key=lambda x: x["score"], reverse=True)
            return scored_events
            
        except Exception as e:
            logger.error(f"Error scoring events batch: {str(e)}")
            return [] 