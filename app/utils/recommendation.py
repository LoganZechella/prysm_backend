import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from app.utils.schema import Event, UserPreferences
from collections import Counter, defaultdict
import math
from functools import lru_cache
import hashlib
from cachetools import TTLCache, cached

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class RecommendationEngine:
    """Engine for generating event recommendations based on similarity and user preferences"""
    
    # Scene categories used for feature vectors
    SCENE_CATEGORIES = [
        "concert_hall",
        "stage",
        "crowd",
        "indoor",
        "art_gallery",
        "outdoor"
    ]
    
    def __init__(self, cache_size: int = 1024, cache_ttl: int = 3600):
        """Initialize the recommendation engine with caching
        
        Args:
            cache_size: Maximum number of items to cache
            cache_ttl: Time to live for cache items in seconds (default 1 hour)
        """
        self._similarity_cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
    
    def get_personalized_recommendations(
        self,
        user_preferences: UserPreferences,
        available_events: List[Event],
        n: int = 10
    ) -> List[Tuple[Event, float]]:
        """Get personalized event recommendations for a user.
        
        Args:
            user_preferences: User's preferences
            available_events: List of available events to choose from
            n: Number of recommendations to return
            
        Returns:
            List of (event, score) tuples, sorted by score descending
        """
        try:
            logger.debug(f"Getting recommendations for user {user_preferences.user_id}")
            logger.debug(f"User preferences: {user_preferences.model_dump_json()}")
            logger.debug(f"Available events: {len(available_events)}")
            
            # First, filter events by user's hard constraints
            filtered_events = []
            for event in available_events:
                # Check price constraints
                if event.price_info.min_price > user_preferences.max_price:
                    logger.debug(f"Event {event.event_id} excluded: price {event.price_info.min_price} > {user_preferences.max_price}")
                    continue
                    
                if user_preferences.price_preferences and event.price_info.price_tier not in user_preferences.price_preferences:
                    logger.debug(f"Event {event.event_id} excluded: tier {event.price_info.price_tier} not in {user_preferences.price_preferences}")
                    continue
                
                # Check location constraints if specified
                if user_preferences.preferred_location and user_preferences.preferred_radius:
                    distance = self._haversine_distance(
                        user_preferences.preferred_location['lat'],
                        user_preferences.preferred_location['lng'],
                        event.location.coordinates['lat'],
                        event.location.coordinates['lng']
                    )
                    if distance > user_preferences.preferred_radius:
                        logger.debug(f"Event {event.event_id} excluded: distance {distance}km > radius {user_preferences.preferred_radius}km")
                        continue
                
                # Check category preferences if specified
                if user_preferences.preferred_categories:
                    has_matching_category = any(
                        cat in user_preferences.preferred_categories 
                        for cat in event.categories
                    )
                    if not has_matching_category:
                        logger.debug(f"Event {event.event_id} excluded: categories {event.categories} don't match preferences {user_preferences.preferred_categories}")
                        continue
                
                filtered_events.append(event)
            
            logger.debug(f"After filtering: {len(filtered_events)} events")
            
            if not filtered_events:
                logger.warning("No events match user preferences")
                return []
            
            # Calculate scores for filtered events
            scored_events = []
            for event in filtered_events:
                score = self._calculate_event_score(event, user_preferences)
                logger.debug(f"Event {event.event_id} score: {score}")
                scored_events.append((event, score))
            
            # Sort by score descending and take top n
            recommendations = sorted(scored_events, key=lambda x: x[1], reverse=True)[:n]
            
            logger.debug(f"Final recommendations: {len(recommendations)}")
            for event, score in recommendations:
                logger.debug(f"- {event.title}: {score}")
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}", exc_info=True)
            return []
    
    def _calculate_event_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate a personalized score for an event based on user preferences"""
        try:
            scores = []
            weights = []
            
            # Category match score
            if preferences.preferred_categories:
                category_matches = sum(1 for cat in event.categories if cat in preferences.preferred_categories)
                category_score = category_matches / len(preferences.preferred_categories)
                scores.append(category_score)
                weights.append(0.4)  # High weight for category matches
            
            # Price score
            if preferences.price_preferences:
                price_score = 1.0 if event.price_info.price_tier in preferences.price_preferences else 0.0
                scores.append(price_score)
                weights.append(0.3)
            
            # Location score
            if preferences.preferred_location and preferences.preferred_radius:
                distance = self._haversine_distance(
                    preferences.preferred_location['lat'],
                    preferences.preferred_location['lng'],
                    event.location.coordinates['lat'],
                    event.location.coordinates['lng']
                )
                location_score = max(0, 1 - (distance / preferences.preferred_radius))
                scores.append(location_score)
                weights.append(0.3)
            
            if not scores:
                return 0.0
                
            # Calculate weighted average
            total_score = sum(s * w for s, w in zip(scores, weights))
            total_weight = sum(weights)
            
            return total_score / total_weight
            
        except Exception as e:
            logger.error(f"Error calculating event score: {str(e)}", exc_info=True)
            return 0.0 
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate Haversine distance between two points in kilometers"""
        R = 6371  # Earth's radius in kilometers
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        return R * c 
        