from typing import List, Dict, Any, Optional, Tuple
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
        
        # Scoring weights
        self.weights = {
            'category': 0.35,
            'location': 0.25,
            'price': 0.15,
            'time': 0.15,
            'popularity': 0.10
        }

    def score_events_batch(
        self,
        events: List[Event],
        preferences: UserPreferences
    ) -> np.ndarray:
        """
        Score multiple events in batch using vectorized operations.
        
        Args:
            events: List of events to score
            preferences: User preferences
            
        Returns:
            Array of scores between 0 and 1 for each event
        """
        if not events:
            return np.array([])
            
        # Pre-compute common values
        now = datetime.utcnow()
        preferred_categories = set(preferences.preferred_categories)
        excluded_categories = set(preferences.excluded_categories)
        
        # Initialize score arrays
        n_events = len(events)
        category_scores = np.zeros(n_events)
        location_scores = np.zeros(n_events)
        price_scores = np.zeros(n_events)
        time_scores = np.zeros(n_events)
        popularity_scores = np.zeros(n_events)
        
        # Batch compute category scores
        for i, event in enumerate(events):
            event_categories = set(event.categories)
            if event_categories & excluded_categories:
                category_scores[i] = 0.0
            elif preferred_categories:
                overlap = len(event_categories & preferred_categories)
                category_scores[i] = min(1.0, overlap / len(preferred_categories))
            else:
                category_scores[i] = 0.5
        
        # Batch compute location scores
        if preferences.preferred_location:
            max_distance = preferences.preferred_location.get('max_distance_km', 50)
            distances = np.array([
                self.location_service.calculate_distance(
                    event.location,
                    preferences.preferred_location
                )
                for event in events
            ])
            location_scores = np.where(
                distances > max_distance,
                0.0,
                1.0 - (distances / max_distance)
            )
        else:
            location_scores.fill(0.5)
        
        # Batch compute price scores
        if preferences.max_price:
            normalized_prices = np.array([
                self.price_normalizer.normalize_price(event.price_info)
                for event in events
            ])
            price_range = preferences.max_price - preferences.min_price
            if price_range <= 0:
                price_scores.fill(1.0)
            else:
                price_scores = np.where(
                    normalized_prices > preferences.max_price,
                    0.0,
                    np.where(
                        normalized_prices < preferences.min_price,
                        0.5,
                        1.0 - ((normalized_prices - preferences.min_price) / price_range)
                    )
                )
        else:
            price_scores.fill(0.5)
        
        # Batch compute time scores
        days_until = np.array([
            (event.start_time - now).days if event.start_time else float('inf')
            for event in events
        ])
        
        time_scores = np.where(
            days_until <= 7,
            0.85,
            np.where(
                days_until <= 30,
                0.85 - (0.5 * (days_until - 7) / 23),
                np.where(
                    days_until <= 90,
                    0.35 - (0.25 * (days_until - 30) / 60),
                    0.1
                )
            )
        )
        
        # Batch compute popularity scores
        view_counts = np.array([
            getattr(event, 'view_count', 0) or 0
            for event in events
        ])
        popularity_scores = np.minimum(1.0, view_counts / 1000)
        
        # Compute weighted sum
        final_scores = (
            self.weights['category'] * category_scores +
            self.weights['location'] * location_scores +
            self.weights['price'] * price_scores +
            self.weights['time'] * time_scores +
            self.weights['popularity'] * popularity_scores
        )
        
        return final_scores

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
        if not events:
            return []
            
        # Score all events in batch
        scores = self.score_events_batch(events, preferences)
        
        # Get indices of top N scores
        if limit:
            top_indices = np.argpartition(scores, -min(limit, len(scores)))[-limit:]
            top_indices = top_indices[np.argsort(scores[top_indices])[::-1]]
        else:
            top_indices = np.argsort(scores)[::-1]
        
        # Return events in order of score
        return [events[i] for i in top_indices]

    def get_trending_recommendations(
        self,
        events: List[Event],
        timeframe_days: int = 7,
        limit: int = 10
    ) -> List[Event]:
        """Get trending events based on recent popularity."""
        if not events:
            return []
            
        now = datetime.utcnow()
        
        # Filter for events within timeframe using vectorized operations
        days_until = np.array([
            (event.start_time - now).days if event.start_time else float('inf')
            for event in events
        ])
        recent_mask = days_until <= timeframe_days
        recent_events = [e for i, e in enumerate(events) if recent_mask[i]]
        
        if not recent_events:
            return []
        
        # Score based on views and recency using vectorized operations
        view_scores = np.minimum(1.0, np.array([
            getattr(event, 'view_count', 0) or 0
            for event in recent_events
        ]) / 1000)
        
        days_until_recent = np.array([
            (event.start_time - now).days
            for event in recent_events
        ])
        recency_scores = 1.0 - (days_until_recent / timeframe_days)
        
        final_scores = (0.7 * view_scores) + (0.3 * recency_scores)
        
        # Get indices of top N scores
        if limit:
            top_indices = np.argpartition(final_scores, -min(limit, len(final_scores)))[-limit:]
            top_indices = top_indices[np.argsort(final_scores[top_indices])[::-1]]
        else:
            top_indices = np.argsort(final_scores)[::-1]
        
        # Return events in order of score
        return [recent_events[i] for i in top_indices] 