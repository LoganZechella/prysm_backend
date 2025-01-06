from typing import List, Dict, Any, Optional
from datetime import datetime
import numpy as np
from sqlalchemy.orm import Session
from app.models.event import Event
from app.models.preferences import UserPreferences
from app.services.location_recommendations import LocationService
from app.services.price_normalization import PriceNormalizer
from app.services.category_extraction import CategoryExtractor

class RecommendationEngine:
    def __init__(self):
        self.location_service = LocationService()
        self.price_normalizer = PriceNormalizer()
        self.category_extractor = CategoryExtractor()

    def score_event(self, event: Event, preferences: UserPreferences) -> float:
        """
        Score an event based on user preferences and event attributes.
        Returns a score between 0 and 1.
        """
        scores = []
        
        # Category match score (0-1)
        category_score = self._calculate_category_score(event, preferences)
        scores.append((category_score, 0.35))  # 35% weight
        
        # Location score (0-1)
        location_score = self._calculate_location_score(event, preferences)
        scores.append((location_score, 0.25))  # 25% weight
        
        # Price score (0-1)
        price_score = self._calculate_price_score(event, preferences)
        scores.append((price_score, 0.15))  # 15% weight
        
        # Time relevance score (0-1)
        time_score = self._calculate_time_score(event, preferences)
        scores.append((time_score, 0.15))  # 15% weight
        
        # Popularity score (0-1)
        popularity_score = self._calculate_popularity_score(event)
        scores.append((popularity_score, 0.10))  # 10% weight
        
        # Calculate weighted average
        final_score = sum(score * weight for score, weight in scores)
        return float(final_score)

    def _calculate_category_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well event categories match user preferences."""
        if not preferences.preferred_categories:
            return 0.5  # Neutral score if no preferences
            
        event_categories = set(event.categories)
        preferred_categories = set(preferences.preferred_categories)
        excluded_categories = set(preferences.excluded_categories)
        
        # Immediate return 0 if event has any excluded categories
        if event_categories & excluded_categories:
            return 0.0
            
        # Calculate overlap between event and preferred categories
        if preferred_categories:
            overlap = len(event_categories & preferred_categories)
            return min(1.0, overlap / len(preferred_categories))
            
        return 0.5

    def _calculate_location_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate location-based score using the location service."""
        if not preferences.preferred_location:
            return 0.5  # Neutral score if no location preference
            
        distance = self.location_service.calculate_distance(
            event.location,
            preferences.preferred_location
        )
        
        max_distance = preferences.preferred_location.get('max_distance_km', 50)
        if distance > max_distance:
            return 0.0
            
        # Linear decay up to max distance
        return 1.0 - (distance / max_distance)

    def _calculate_price_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate price-based score using price normalizer."""
        if not event.price_info or not preferences.max_price:
            return 0.5  # Neutral score if price info missing
            
        normalized_price = self.price_normalizer.normalize_price(event.price_info)
        
        if normalized_price < preferences.min_price:
            return 0.5  # Below minimum price is neutral
        if normalized_price > preferences.max_price:
            return 0.0  # Above maximum price is zero
            
        # Linear score between min and max price
        price_range = preferences.max_price - preferences.min_price
        if price_range <= 0:
            return 1.0
        return 1.0 - ((normalized_price - preferences.min_price) / price_range)

    def _calculate_time_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate time-based relevance score."""
        if not event.start_time:
            return 0.5  # Neutral score if no time info
            
        now = datetime.utcnow()
        days_until_event = (event.start_time - now).days
        
        # Immediate future events (0-7 days) get high scores
        if 0 <= days_until_event <= 7:
            return 0.85  # High but not maximum score
        # Events 8-30 days away get decreasing scores
        elif 8 <= days_until_event <= 30:
            return 0.85 - (0.5 * (days_until_event - 7) / 23)  # Steeper decay
        # Events 31-90 days away get lower decreasing scores
        elif 31 <= days_until_event <= 90:
            return 0.35 - (0.25 * (days_until_event - 30) / 60)  # Lower base score
        # Events more than 90 days away get lowest scores
        else:
            return 0.1

    def _calculate_popularity_score(self, event: Event) -> float:
        """Calculate popularity-based score."""
        # Normalize views/interactions if available
        if hasattr(event, 'view_count') and event.view_count is not None:
            return min(1.0, event.view_count / 1000)  # Arbitrary normalization
        return 0.5  # Neutral score if no popularity data

    def get_personalized_recommendations(
        self,
        preferences: UserPreferences,
        events: List[Event],
        limit: int = 10
    ) -> List[Event]:
        """
        Generate personalized event recommendations for a user.
        
        Args:
            preferences: User's preference settings
            events: List of available events to choose from
            limit: Maximum number of recommendations to return
            
        Returns:
            List of recommended events, ordered by relevance
        """
        # Score all events
        scored_events = [
            (event, self.score_event(event, preferences))
            for event in events
        ]
        
        # Sort by score (descending) and return top N events
        scored_events.sort(key=lambda x: x[1], reverse=True)
        return [event for event, _ in scored_events[:limit]]

    def get_trending_recommendations(
        self,
        events: List[Event],
        timeframe_days: int = 7,
        limit: int = 10
    ) -> List[Event]:
        """Get trending events based on recent popularity."""
        now = datetime.utcnow()
        
        # Filter for events within timeframe
        recent_events = [
            event for event in events
            if event.start_time and (event.start_time - now).days <= timeframe_days
        ]
        
        # Score based on views and recency
        scored_events = []
        for event in recent_events:
            view_score = min(1.0, (event.view_count or 0) / 1000)
            days_away = (event.start_time - now).days
            recency_score = 1.0 - (days_away / timeframe_days)
            final_score = (0.7 * view_score) + (0.3 * recency_score)
            scored_events.append((event, final_score))
        
        # Sort by score and return top N
        scored_events.sort(key=lambda x: x[1], reverse=True)
        return [event for event, _ in scored_events[:limit]] 