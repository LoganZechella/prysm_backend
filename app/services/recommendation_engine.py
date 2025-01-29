"""Recommendation engine service for event recommendations."""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
from app.models.event import EventModel
from app.services.location_recommendations import LocationService
from app.utils.deduplication import calculate_title_similarity

logger = logging.getLogger(__name__)

class RecommendationEngine:
    """Engine for generating personalized event recommendations."""
    
    def __init__(self, db: Session):
        """Initialize recommendation engine."""
        self.db = db
        self.default_max_distance = 50.0  # km
        self.default_max_results = 50
        self.default_min_similarity = 0.8
        self.location_service = LocationService()
        
    async def get_recommendations(
        self,
        user_id: str,
        user_location: Dict[str, Any],
        preferences: Optional[Dict[str, Any]] = None,
        max_distance: Optional[float] = None,
        max_results: Optional[int] = None
    ) -> List[EventModel]:
        """
        Get personalized event recommendations for a user.
        
        Args:
            user_id: User ID
            user_location: User's location (lat/lon or address)
            preferences: Optional user preferences
            max_distance: Optional maximum distance in km
            max_results: Optional maximum number of results
            
        Returns:
            List of recommended events
        """
        try:
            # Get base query for upcoming events
            base_query = self.db.query(EventModel).filter(
                EventModel.start_datetime >= datetime.utcnow()
            )
            
            # Apply location filter
            events = base_query.all()
            max_dist = max_distance or self.default_max_distance
            
            # Get user coordinates if needed
            user_coords = self.location_service.get_coordinates(user_location)
            if not user_coords:
                logger.error(f"Could not get coordinates for user location: {user_location}")
                return []
                
            filtered_events = self.location_service.filter_by_distance(events, user_coords, max_dist)
            
            # Apply preference filters if provided
            if preferences:
                filtered_events = self._apply_preferences(filtered_events, preferences)
            
            # Sort by relevance score
            scored_events = self._score_events(filtered_events, user_coords, preferences)
            
            # Return top N results
            max_res = max_results or self.default_max_results
            return scored_events[:max_res]
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            raise
            
    def _score_events(
        self,
        events: List[Dict[str, Any]],
        user_location: Dict[str, float],
        preferences: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Score events based on relevance to user."""
        scored_events = []
        for event in events:
            score = self._calculate_event_score(event, user_location, preferences)
            event["relevance_score"] = score
            scored_events.append(event)
            
        return sorted(scored_events, key=lambda x: x["relevance_score"], reverse=True)
        
    def _calculate_event_score(
        self,
        event: Dict[str, Any],
        user_location: Dict[str, float],
        preferences: Optional[Dict[str, Any]] = None
    ) -> float:
        """Calculate relevance score for an event."""
        score = 0.0
        weights = {
            "distance": 0.4,
            "category": 0.3,
            "price": 0.2,
            "popularity": 0.1
        }
        
        # Distance score (inverse relationship)
        venue_coords = self.location_service._get_event_coordinates(event)
        if venue_coords and all(venue_coords.values()):
            distance = self.location_service.calculate_distance(user_location, venue_coords)
            max_dist = self.default_max_distance
            distance_score = max(0, 1 - (distance / max_dist))
            score += weights["distance"] * distance_score
            
        # Add other scoring components based on preferences
        if preferences:
            # Category match
            if "preferred_categories" in preferences:
                category_score = self._calculate_category_score(
                    event.get("categories", []),
                    preferences["preferred_categories"]
                )
                score += weights["category"] * category_score
                
            # Price match
            if "price_range" in preferences:
                price_score = self._calculate_price_score(
                    event.get("price_info", {}),
                    preferences["price_range"]
                )
                score += weights["price"] * price_score
                
        # Popularity score
        popularity_score = min(1.0, event.get("rsvp_count", 0) / 100)
        score += weights["popularity"] * popularity_score
        
        return score
        
    def _calculate_category_score(
        self,
        event_categories: List[str],
        preferred_categories: List[str]
    ) -> float:
        """Calculate how well event categories match user preferences."""
        if not event_categories or not preferred_categories:
            return 0.0
            
        matches = sum(1 for cat in event_categories if cat in preferred_categories)
        return matches / max(len(event_categories), len(preferred_categories))
        
    def _calculate_price_score(
        self,
        price_info: Dict[str, Any],
        preferred_range: Dict[str, float]
    ) -> float:
        """Calculate how well event price matches user preferences."""
        if not price_info or "min_price" not in price_info:
            return 0.5  # Neutral score for unknown price
            
        event_price = price_info["min_price"]
        max_price = preferred_range.get("max", float("inf"))
        
        if event_price > max_price:
            return 0.0
            
        # Score based on how close to preferred range
        price_score = 1.0 - (event_price / max_price)
        return max(0.0, min(1.0, price_score))

    def _apply_preferences(
        self,
        events: List[EventModel],
        preferences: Dict[str, Any]
    ) -> List[EventModel]:
        """Apply user preferences to filter events."""
        filtered = events.copy()
        
        # Filter by categories if specified
        if 'categories' in preferences:
            filtered = [
                event for event in filtered
                if any(cat in event.categories for cat in preferences['categories'])
            ]
        
        # Filter by price range if specified
        if 'max_price' in preferences:
            filtered = [
                event for event in filtered
                if event.price_info and event.price_info.get('normalized_price', float('inf')) <= preferences['max_price']
            ]
        
        # Filter by date range if specified
        if 'date_range' in preferences:
            start = preferences['date_range'].get('start')
            end = preferences['date_range'].get('end')
            
            if start:
                filtered = [
                    event for event in filtered
                    if event.start_datetime >= start
                ]
            
            if end:
                filtered = [
                    event for event in filtered
                    if event.start_datetime <= end
                ]
        
        return filtered
    
    async def get_similar_events(
        self,
        event_id: str,
        max_results: Optional[int] = None,
        min_similarity: Optional[float] = None
    ) -> List[EventModel]:
        """
        Get events similar to a given event.
        
        Args:
            event_id: Reference event ID
            max_results: Optional maximum number of results
            min_similarity: Optional minimum similarity threshold
            
        Returns:
            List of similar events
        """
        try:
            # Get reference event
            ref_event = self.db.query(EventModel).filter(
                EventModel.id == event_id
            ).first()
            
            if not ref_event:
                raise ValueError(f"Event {event_id} not found")
            
            # Get candidate events (same city, upcoming)
            candidates = self.db.query(EventModel).filter(
                EventModel.venue_city == ref_event.venue_city,
                EventModel.start_datetime >= datetime.utcnow(),
                EventModel.id != event_id
            ).all()
            
            # Calculate similarities
            similar_events = []
            min_sim = min_similarity or self.default_min_similarity
            
            for event in candidates:
                # Compare titles
                title_sim = calculate_title_similarity(ref_event.title, event.title)
                
                # Compare categories
                cat_sim = len(set(ref_event.categories) & set(event.categories)) / \
                    len(set(ref_event.categories) | set(event.categories)) if event.categories else 0
                
                # Calculate overall similarity
                similarity = 0.7 * title_sim + 0.3 * cat_sim
                
                if similarity >= min_sim:
                    similar_events.append((event, similarity))
            
            # Sort by similarity and return top N
            similar_events.sort(key=lambda x: x[1], reverse=True)
            max_res = max_results or self.default_max_results
            return [event for event, _ in similar_events[:max_res]]
            
        except Exception as e:
            logger.error(f"Error getting similar events: {str(e)}")
            raise
    
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