"""Core recommendation engine implementation."""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from sqlalchemy.dialects.postgresql import JSONB
from app.models.event import Event
from app.models.preferences import UserPreferences
from app.services.location_recommendations import LocationService
from app.services.price_normalization import PriceNormalizer
from app.services.category_extraction import CategoryExtractor
from app.models.traits import Traits
from app.services.trait_extractor import TraitExtractor
from app.recommendation.filters.content import ContentBasedFilter

logger = logging.getLogger(__name__)

class RecommendationEngine:
    """Core recommendation engine for generating personalized event recommendations."""
    
    def __init__(self, db: Session):
        """Initialize the recommendation engine."""
        self.db = db
        self.location_service = LocationService()
        self.price_normalizer = PriceNormalizer()
        self.category_extractor = CategoryExtractor()
        self.trait_extractor = TraitExtractor(db)
        self.content_filter = ContentBasedFilter()
        
        # Scoring weights
        self.weights = {
            'category': 0.35,
            'location': 0.25,
            'price': 0.15,
            'time': 0.15,
            'popularity': 0.10
        }

    async def get_recommendations(
        self,
        user_id: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get personalized event recommendations for a user."""
        try:
            # Get user traits
            traits = self.db.query(Traits).filter_by(user_id=user_id).first()
            if not traits:
                logger.warning(f"No traits found for user {user_id}")
                return []

            # Process user traits
            professional_traits = traits.professional_traits or {}
            interest_traits = professional_traits.get("interests", [])
            location_traits = self._process_location_traits(professional_traits)

            # Get candidate events
            candidate_events = await self._get_candidate_events(
                categories=interest_traits,
                location=location_traits,
                filters=filters or {}
            )

            if not candidate_events:
                logger.info("No candidate events found")
                return []

            # Score and rank events
            scored_events = await self._score_events(candidate_events, professional_traits)

            # Apply minimum score filter if specified
            min_score = filters.get("min_score", 0.0) if filters else 0.0
            scored_events = [
                event for event in scored_events
                if event["score"] >= min_score
            ]

            # Sort by score and start time
            scored_events.sort(key=lambda x: (-x["score"], x["start_time"]))

            return scored_events

        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            return []

    def _process_location_traits(self, professional_traits: Dict[str, Any]) -> Dict[str, str]:
        """Process location traits from professional traits."""
        location = professional_traits.get("location", {})
        if not location:
            logger.warning("No location found in professional traits")
            return {"country": "US"}  # Default to US if no location found
        return location

    async def _get_candidate_events(
        self,
        categories: List[str],
        location: Dict[str, str],
        filters: Dict[str, Any]
    ) -> List[Event]:
        """Get candidate events based on user preferences and filters."""
        try:
            # Start with base query
            query = self.db.query(Event)

            # Filter future events
            query = query.filter(Event.start_time >= datetime.utcnow())

            # Filter by categories if provided
            if categories:
                category_conditions = [
                    func.array_to_string(Event.categories, ',', '').ilike(f"%{category}%")
                    for category in categories
                ]
                query = query.filter(or_(*category_conditions))

            # Filter by location if provided
            if location:
                query = query.filter(Event.location.cast(JSONB).contains({"country": location["country"]}))

            # Filter by max price if provided
            if filters.get("max_price"):
                query = query.filter(
                    Event.price_info.cast(JSONB).contains({"amount": filters["max_price"]})
                )

            # Filter by sources if provided
            if filters.get("sources"):
                query = query.filter(Event.source.in_(filters["sources"]))

            # Order by start time and limit results
            query = query.order_by(Event.start_time).limit(100)

            return query.all()

        except Exception as e:
            logger.error(f"Error getting candidate events: {str(e)}")
            return []

    async def _score_events(
        self,
        events: List[Event],
        professional_traits: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Score events based on user preferences."""
        scored_events = []

        for event in events:
            try:
                # Calculate similarity score using content-based filter
                score = self.content_filter.calculate_similarity(
                    event_features=event.get_feature_vector(),
                    user_traits=professional_traits
                )

                # Create event dict with score and explanation
                event_dict = event.to_dict()
                event_dict["score"] = score
                event_dict["explanation"] = self._generate_explanation(
                    event, professional_traits, score
                )

                scored_events.append(event_dict)

            except Exception as e:
                logger.error(f"Error scoring event {event.id}: {str(e)}")
                continue

        return scored_events

    def _generate_explanation(
        self,
        event: Event,
        professional_traits: Dict[str, Any],
        score: float
    ) -> str:
        """Generate human-readable explanation for recommendation."""
        reasons = []

        # Check category matches
        user_interests = professional_traits.get("interests", [])
        matching_categories = [
            cat for cat in event.categories
            if any(interest.lower() in cat.lower() for interest in user_interests)
        ]
        if matching_categories:
            reasons.append(
                f"Matches your interests in {', '.join(matching_categories)}"
            )

        # Check location match
        user_location = professional_traits.get("location", {}).get("country")
        event_location = event.location.get("country")
        if user_location and event_location and user_location == event_location:
            reasons.append(f"Located in {event_location}")

        # Add time-based reason
        time_until_event = event.start_time - datetime.utcnow()
        if time_until_event <= timedelta(days=7):
            reasons.append("Happening this week")
        elif time_until_event <= timedelta(days=30):
            reasons.append("Happening this month")

        if not reasons:
            return "Based on your general preferences"

        return "; ".join(reasons)

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