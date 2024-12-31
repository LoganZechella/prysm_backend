from datetime import datetime
from typing import List, Dict, Any, Set
import pandas as pd
from geopy.distance import geodesic
from app.models.profile import UserProfile

class RecommendationEngine:
    def __init__(self, events_df: pd.DataFrame):
        """
        Initialize the recommendation engine with a DataFrame of events.
        
        Args:
            events_df: DataFrame containing event data with columns:
                - event_id
                - title
                - description
                - start_datetime
                - location (dict with lat/lng)
                - categories
                - price_info
        """
        self.events_df = events_df
    
    def _calculate_distance(self, user_location: Dict[str, float], event_location: Dict[str, float]) -> float:
        """Calculate distance between user and event in kilometers."""
        if not all(k in event_location for k in ['lat', 'lng']):
            return float('inf')
        
        user_coords = (user_location['lat'], user_location['lng'])
        event_coords = (event_location['lat'], event_location['lng'])
        return geodesic(user_coords, event_coords).kilometers
    
    def _is_time_match(self, event_time: datetime, preferred_times: List[str]) -> bool:
        """Check if event time matches user preferences."""
        hour = event_time.hour
        if hour >= 5 and hour < 12 and 'morning' in preferred_times:
            return True
        if hour >= 12 and hour < 17 and 'afternoon' in preferred_times:
            return True
        if hour >= 17 and hour < 23 and 'evening' in preferred_times:
            return True
        return False
    
    def _calculate_category_match_score(self, event_categories: List[str], user_interests: List[str], 
                                      preferred_categories: List[str]) -> float:
        """Calculate category match score between 0 and 1."""
        if not event_categories:
            return 0.0
        
        # Convert lists to sets for intersection operation
        user_preferences: Set[str] = set(user_interests + preferred_categories)
        event_cats: Set[str] = set(event_categories)
        
        # Calculate intersection score
        intersection = len(event_cats.intersection(user_preferences))
        return intersection / len(event_categories)
    
    def get_recommendations(self, profile: UserProfile, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get event recommendations for a user profile.
        
        Args:
            profile: UserProfile object
            limit: Maximum number of recommendations to return
            
        Returns:
            List of recommended events
        """
        recommendations = []
        
        for _, event in self.events_df.iterrows():
            # Skip if event is in excluded categories
            if any(cat in profile.excluded_categories for cat in event['categories']):
                continue
            
            # Skip if price is outside preferences
            if profile.event_preferences.min_price is not None:
                if event['price_info']['min_price'] < profile.event_preferences.min_price:
                    continue
            if profile.event_preferences.max_price is not None:
                if event['price_info']['min_price'] > profile.event_preferences.max_price:
                    continue
            
            # Calculate distance
            distance = self._calculate_distance(
                event['location']['coordinates'],
                {'lat': event['location']['coordinates']['lat'], 'lng': event['location']['coordinates']['lng']}
            )
            if distance > profile.location_preference.max_distance_km:
                continue
            
            # Check day preference
            event_day = pd.to_datetime(event['start_datetime']).strftime('%A')
            if event_day not in profile.event_preferences.preferred_days:
                continue
            
            # Check time preference
            if not self._is_time_match(
                pd.to_datetime(event['start_datetime']),
                profile.event_preferences.preferred_times
            ):
                continue
            
            # Calculate category match score
            category_score = self._calculate_category_match_score(
                event['categories'],
                profile.interests,
                profile.event_preferences.categories
            )
            
            # Only include events with some category match
            if category_score > 0:
                recommendations.append({
                    'event_id': event['event_id'],
                    'title': event['title'],
                    'description': event['description'],
                    'start_datetime': event['start_datetime'],
                    'location': event['location'],
                    'categories': event['categories'],
                    'price_info': event['price_info'],
                    'distance_km': distance,
                    'match_score': category_score
                })
        
        # Sort by match score and distance
        recommendations.sort(key=lambda x: (-x['match_score'], x['distance_km']))
        
        return recommendations[:limit] 