import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
from app.utils.schema import Event, UserPreferences
from collections import Counter, defaultdict
import math

logger = logging.getLogger(__name__)

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
    
    def __init__(self):
        """Initialize the recommendation engine"""
        pass
    
    def calculate_event_similarity(self, event1: Event, event2: Event) -> float:
        """Calculate similarity score between two events.
        
        Uses multiple factors:
        - Category overlap
        - Scene similarity from image analysis
        - Price tier similarity
        - Location proximity
        - Temporal proximity
        """
        try:
            # Calculate category similarity
            cat_sim = self._calculate_category_similarity(event1, event2)
            
            # Calculate scene similarity from image analysis
            scene_sim = self._calculate_scene_similarity(event1, event2)
            
            # Calculate price similarity
            price_sim = self._calculate_price_similarity(event1, event2)
            
            # Calculate location similarity
            loc_sim = self._calculate_location_similarity(event1, event2)
            
            # Calculate temporal similarity
            time_sim = self._calculate_temporal_similarity(event1, event2)
            
            # Weighted combination of similarities
            weights = {
                'category': 0.3,
                'scene': 0.2,
                'price': 0.15,
                'location': 0.2,
                'time': 0.15
            }
            
            similarity = (
                weights['category'] * cat_sim +
                weights['scene'] * scene_sim +
                weights['price'] * price_sim +
                weights['location'] * loc_sim +
                weights['time'] * time_sim
            )
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error calculating event similarity: {str(e)}")
            return 0.0
    
    def _calculate_category_similarity(self, event1: Event, event2: Event) -> float:
        """Calculate similarity based on event categories"""
        try:
            # Get all categories including ancestors
            cats1 = set(event1.categories)
            cats2 = set(event2.categories)
            
            # Add hierarchical categories
            for cat in event1.categories:
                if cat in event1.category_hierarchy:
                    cats1.update(event1.category_hierarchy[cat])
            for cat in event2.categories:
                if cat in event2.category_hierarchy:
                    cats2.update(event2.category_hierarchy[cat])
            
            # Calculate weighted Jaccard similarity
            # Give more weight to direct category matches
            direct_matches = len(set(event1.categories).intersection(event2.categories))
            hierarchy_matches = len(cats1.intersection(cats2)) - direct_matches
            
            total_categories = len(cats1.union(cats2))
            if total_categories == 0:
                return 0.0
            
            # Weight direct matches twice as much as hierarchy matches
            similarity = (2 * direct_matches + hierarchy_matches) / (2 * total_categories)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error calculating category similarity: {str(e)}")
            return 0.0
    
    def _calculate_scene_similarity(self, event1: Event, event2: Event) -> float:
        """Calculate similarity between events based on scene classification"""
        if not event1.image_analysis or not event2.image_analysis:
            return 0.0
            
        scene1 = event1.image_analysis[0].scene_classification
        scene2 = event2.image_analysis[0].scene_classification
        
        if not scene1 or not scene2:
            return 0.0
            
        vector1 = self._get_scene_vector(scene1)
        vector2 = self._get_scene_vector(scene2)
        
        # Calculate cosine similarity between scene vectors
        dot_product = sum(a * b for a, b in zip(vector1, vector2))
        norm1 = math.sqrt(sum(a * a for a in vector1))
        norm2 = math.sqrt(sum(b * b for b in vector2))
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
            
        return dot_product / (norm1 * norm2)
    
    def _get_scene_vector(self, scene_classification: Dict[str, float]) -> List[float]:
        """Convert scene classification to feature vector"""
        vector = [0.0] * len(self.SCENE_CATEGORIES)
        for scene, score in scene_classification.items():
            if scene in self.SCENE_CATEGORIES:
                vector[self.SCENE_CATEGORIES.index(scene)] = score
        return vector
    
    def _calculate_price_similarity(self, event1: Event, event2: Event) -> float:
        """Calculate similarity based on price tier"""
        try:
            # Price tier mapping
            tier_map = {
                'free': 0,
                'budget': 1,
                'medium': 2,
                'premium': 3
            }
            
            tier1 = tier_map.get(event1.price_info.price_tier, 2)  # Default to medium
            tier2 = tier_map.get(event2.price_info.price_tier, 2)
            
            # Calculate similarity (1 - normalized difference)
            max_diff = max(tier_map.values()) - min(tier_map.values())
            similarity = 1 - (abs(tier1 - tier2) / max_diff)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error calculating price similarity: {str(e)}")
            return 0.0
    
    def _calculate_location_similarity(self, event1: Event, event2: Event) -> float:
        """Calculate similarity based on location proximity"""
        try:
            # Calculate Haversine distance
            lat1 = event1.location.coordinates['lat']
            lon1 = event1.location.coordinates['lng']
            lat2 = event2.location.coordinates['lat']
            lon2 = event2.location.coordinates['lng']
            
            distance = self._haversine_distance(lat1, lon1, lat2, lon2)
            
            # Convert distance to similarity score
            # Using sigmoid function for smoother decay
            # Scale factor of 10km for urban events
            similarity = 1 / (1 + np.exp(distance/10 - 2))
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error calculating location similarity: {str(e)}")
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
    
    def _calculate_temporal_similarity(self, event1: Event, event2: Event) -> float:
        """Calculate similarity based on temporal proximity"""
        try:
            # Calculate time difference in hours
            time_diff = abs((event1.start_datetime - event2.start_datetime).total_seconds() / 3600)
            
            # Convert to similarity score using exponential decay
            # Using 24 hours as scale factor
            similarity = np.exp(-time_diff/24)
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error calculating temporal similarity: {str(e)}")
            return 0.0
    
    def get_similar_events(self, event: Event, candidates: List[Event], n: int = 5) -> List[Tuple[Event, float]]:
        """Get n most similar events to the given event"""
        try:
            # Calculate similarities
            similarities = []
            for candidate in candidates:
                if candidate.event_id != event.event_id:
                    # Check for category overlap first
                    common_categories = set(event.categories) & set(candidate.categories)
                    if not common_categories:
                        continue
                        
                    similarity = self.calculate_event_similarity(event, candidate)
                    similarities.append((candidate, similarity))
            
            # Sort by similarity and return top n
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:n]
            
        except Exception as e:
            logger.error(f"Error getting similar events: {str(e)}")
            return []
    
    def get_personalized_recommendations(
        self,
        user_preferences: UserPreferences,
        available_events: List[Event],
        n: int = 10
    ) -> List[Tuple[Event, float]]:
        """Get personalized event recommendations for a user"""
        try:
            # Calculate scores for each event
            scores = []
            for event in available_events:
                score = self._calculate_preference_score(event, user_preferences)
                scores.append((event, score))
            
            # Sort by score and return top n
            scores.sort(key=lambda x: x[1], reverse=True)
            return scores[:n]
            
        except Exception as e:
            logger.error(f"Error getting personalized recommendations: {str(e)}")
            return []
    
    def _calculate_preference_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well an event matches user preferences"""
        try:
            score = 0.0
            weights = {
                'categories': 0.4,
                'price': 0.2,
                'location': 0.2,
                'time': 0.2
            }
            
            # Category match - give points for any matching category
            event_cats = set(event.categories)
            pref_cats = set(preferences.preferred_categories)
            cat_overlap = len(event_cats.intersection(pref_cats))
            cat_score = min(1.0, cat_overlap / max(1, min(len(event_cats), len(pref_cats))))
            
            # Price match
            price_match = event.price_info.price_tier in preferences.price_preferences
            price_score = 1.0 if price_match else 0.0
            
            # Location match (within preferred radius)
            location_score = self._check_location_match(event, preferences)
            
            # Time match (within preferred times)
            time_score = self._check_time_match(event, preferences)
            
            # Combine scores
            score = (
                weights['categories'] * cat_score +
                weights['price'] * price_score +
                weights['location'] * location_score +
                weights['time'] * time_score
            )
            
            return score
            
        except Exception as e:
            logger.error(f"Error calculating preference score: {str(e)}")
            return 0.0
    
    def _check_location_match(self, event: Event, preferences: UserPreferences) -> float:
        """Check if event location matches user preferences"""
        try:
            if not preferences.preferred_location:
                return 0.5
            
            distance = self._haversine_distance(
                event.location.coordinates['lat'],
                event.location.coordinates['lng'],
                preferences.preferred_location['lat'],
                preferences.preferred_location['lng']
            )
            
            # Convert to score based on preferred radius
            if distance <= preferences.preferred_radius:
                return 1.0
            else:
                return max(0.0, 1.0 - (distance - preferences.preferred_radius) / preferences.preferred_radius)
            
        except Exception as e:
            logger.error(f"Error checking location match: {str(e)}")
            return 0.0
    
    def _check_time_match(self, event: Event, preferences: UserPreferences) -> float:
        """Check if event time matches user preferences"""
        try:
            event_time = event.start_datetime.time()
            
            # Check if event time falls within any preferred time slots
            for start, end in preferences.preferred_times:
                if start <= event_time <= end:
                    return 1.0
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error checking time match: {str(e)}")
            return 0.0 