from typing import List, Tuple, Dict, Optional
from datetime import datetime, time
import numpy as np
from geopy.distance import geodesic
from app.schemas import (
    Event,
    UserPreferences,
    Location,
    PriceInfo,
    EventAttributes
)

class RecommendationService:
    def calculate_category_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well an event's categories match user preferences."""
        if not preferences.preferred_categories:
            return 1.0  # No preferences means all categories are acceptable
        
        matching_categories = set(event.categories) & set(preferences.preferred_categories)
        if not matching_categories:
            return 0.0
        
        return len(matching_categories) / len(preferences.preferred_categories)

    def calculate_location_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well an event's location matches user preferences."""
        if not preferences.preferred_location:
            return 1.0  # No location preference means all locations are acceptable
        
        event_coords = (event.location.coordinates["lat"], event.location.coordinates["lng"])
        user_coords = (preferences.preferred_location["lat"], preferences.preferred_location["lng"])
        
        distance = geodesic(event_coords, user_coords).miles
        
        if distance > preferences.max_distance:
            return 0.0
        
        # Linear decay: score decreases as distance increases
        return 1.0 - (distance / preferences.max_distance)

    def calculate_price_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well an event's price matches user preferences."""
        if not event.price_info:
            return 1.0  # Free events or events without price info get full score
        
        # Check price range
        if (event.price_info.min_price > preferences.max_price or 
            event.price_info.max_price < preferences.min_price):
            return 0.0
        
        # Check price tier preference
        if preferences.price_preferences:
            if event.price_info.price_tier not in preferences.price_preferences:
                return 0.5  # Partial match if tier doesn't match but price is in range
        
        return 1.0

    def calculate_time_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well an event's time matches user preferences."""
        event_time = event.start_datetime.time()
        event_day = event.start_datetime.strftime("%A").lower()
        
        day_score = 1.0
        if preferences.preferred_days:
            day_score = 1.0 if event_day in preferences.preferred_days else 0.5
        
        time_score = 1.0
        if preferences.preferred_times:
            time_score = 0.0
            for start, end in preferences.preferred_times:
                if start <= event_time <= end:
                    time_score = 1.0
                    break
        
        return (day_score + time_score) / 2

    def calculate_attribute_score(self, event: Event, preferences: UserPreferences) -> float:
        """Calculate how well an event's attributes match user preferences."""
        scores = []
        
        # Indoor/Outdoor preference
        if preferences.indoor_outdoor_preference:
            scores.append(1.0 if event.attributes.indoor_outdoor == preferences.indoor_outdoor_preference else 0.0)
        
        # Accessibility requirements
        if preferences.accessibility_requirements:
            accessibility_score = len(set(preferences.accessibility_requirements) & 
                                   set(event.attributes.accessibility_features)) / len(preferences.accessibility_requirements)
            scores.append(accessibility_score)
        
        if not scores:
            return 1.0  # No attribute preferences means full score
        
        return float(np.mean(scores))

    def get_personalized_recommendations(
        self,
        preferences: UserPreferences,
        events: List[Event],
        limit: Optional[int] = None
    ) -> List[Event]:
        """Generate personalized event recommendations based on user preferences."""
        scored_events = []
        
        for event in events:
            category_score = self.calculate_category_score(event, preferences)
            if category_score == 0.0:
                continue  # Skip events that don't match any preferred categories
            
            location_score = self.calculate_location_score(event, preferences)
            if location_score == 0.0:
                continue  # Skip events that are too far
            
            price_score = self.calculate_price_score(event, preferences)
            time_score = self.calculate_time_score(event, preferences)
            attribute_score = self.calculate_attribute_score(event, preferences)
            
            # Calculate weighted average score
            total_score = (
                category_score * 0.3 +
                location_score * 0.25 +
                price_score * 0.2 +
                time_score * 0.15 +
                attribute_score * 0.1
            )
            
            scored_events.append((event, total_score))
        
        # Sort by score in descending order
        scored_events.sort(key=lambda x: x[1], reverse=True)
        
        # Return only the events, without scores
        recommended_events = [event for event, _ in scored_events]
        
        if limit:
            recommended_events = recommended_events[:limit]
        
        return recommended_events 