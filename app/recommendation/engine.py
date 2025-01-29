"""Core recommendation engine implementation."""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from sqlalchemy.dialects.postgresql import JSONB
from app.models.event import EventModel
from app.models.preferences import UserPreferences
from app.services.location_recommendations import LocationService
from app.services.price_normalization import PriceNormalizer
from app.services.category_extraction import CategoryExtractor
from app.models.traits import Traits
from app.services.trait_extractor import TraitExtractor
from app.recommendation.filters.content import ContentBasedFilter
from app.recommendation.models.recommendation_model import RecommendationModel

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
        self.recommendation_model = RecommendationModel()

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

            # Get candidate events
            candidate_events = await self._get_candidate_events(
                categories=traits.professional_traits.get("interests", []),
                location=self._process_location_traits(traits.professional_traits),
                filters=filters or {}
            )

            if not candidate_events:
                logger.info("No candidate events found")
                return []

            # Prepare context for scoring
            context = self._prepare_scoring_context(filters)

            # Score events using ML model
            scored_events = self.recommendation_model.score_events(
                user_traits=traits,
                events=candidate_events,
                context=context
            )

            # Convert to response format
            recommendations = []
            for event, score, explanations in scored_events:
                if score >= (filters.get("min_score", 0.0) if filters else 0.0):
                    recommendations.append({
                        "event": event,
                        "score": score,
                        "explanations": explanations
                    })

            return recommendations

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
    ) -> List[EventModel]:
        """Get candidate events based on user preferences and filters."""
        try:
            # Start with base query
            query = self.db.query(EventModel)

            # Filter future events
            query = query.filter(EventModel.start_datetime >= datetime.utcnow())

            # Filter by categories if provided
            if categories:
                category_conditions = [
                    func.array_to_string(EventModel.categories, ',', '').ilike(f"%{category}%")
                    for category in categories
                ]
                query = query.filter(or_(*category_conditions))

            # Filter by location if provided
            if location:
                query = query.filter(
                    and_(
                        EventModel.venue_city == location.get('city'),
                        EventModel.venue_state == location.get('state'),
                        EventModel.venue_country == location.get('country')
                    )
                )

            # Apply additional filters
            query = self._apply_filters(query, filters)

            # Order by start time and limit results
            query = query.order_by(EventModel.start_datetime).limit(100)

            return query.all()

        except Exception as e:
            logger.error(f"Error getting candidate events: {str(e)}")
            return []

    def _apply_filters(self, query, filters: Dict[str, Any]):
        """Apply additional filters to the query."""
        if not filters:
            return query

        try:
            # Filter by max price
            if filters.get("max_price"):
                query = query.filter(
                    EventModel.price_info['max_price'].astext.cast(float) <= filters["max_price"]
                )

            # Filter by platforms
            if filters.get("platforms"):
                query = query.filter(EventModel.platform.in_(filters["platforms"]))

            # Filter by date range
            if filters.get("start_date"):
                query = query.filter(EventModel.start_datetime >= filters["start_date"])
            if filters.get("end_date"):
                query = query.filter(EventModel.start_datetime <= filters["end_date"])

            # Filter by event type
            if filters.get("event_types"):
                query = query.filter(EventModel.event_type.in_(filters["event_types"]))

            return query

        except Exception as e:
            logger.error(f"Error applying filters: {str(e)}")
            return query

    def _prepare_scoring_context(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Prepare context dictionary for event scoring."""
        context = {}
        
        if filters:
            # Normalize price filter
            if filters.get("max_price"):
                context["max_price_normalized"] = self.price_normalizer.normalize_price(
                    filters["max_price"]
                )
            
            # Add time preferences
            if filters.get("preferred_times"):
                context["preferred_times"] = filters["preferred_times"]
            
            # Add location preferences
            if filters.get("max_distance"):
                context["max_distance"] = filters["max_distance"]
            
            # Add any other relevant context from filters
            context["filters"] = filters
        
        return context

    def get_personalized_recommendations(
        self,
        events: List[EventModel],
        preferences: UserPreferences,
        limit: int = 10
    ) -> List[EventModel]:
        """Get personalized recommendations from a list of events."""
        try:
            # Get user traits
            traits = self.trait_extractor.get_traits(preferences.user_id)
            if not traits:
                logger.warning(f"No traits found for user {preferences.user_id}")
                return events[:limit]  # Return top events without personalization

            # Score events using ML model
            scored_events = self.recommendation_model.score_events(
                user_traits=traits,
                events=events
            )

            # Return top scoring events
            return [event for event, _, _ in scored_events[:limit]]

        except Exception as e:
            logger.error(f"Error getting personalized recommendations: {str(e)}")
            return events[:limit]

    def get_trending_recommendations(
        self,
        events: List[EventModel],
        timeframe_days: int = 7,
        limit: int = 10
    ) -> List[EventModel]:
        """Get trending recommendations based on recent engagement."""
        try:
            # Filter events within timeframe
            recent_cutoff = datetime.utcnow() - timedelta(days=timeframe_days)
            recent_events = [
                event for event in events
                if event.start_datetime >= recent_cutoff
            ]

            if not recent_events:
                return []

            # Score events based on engagement metrics
            scored_events = []
            for event in recent_events:
                engagement_score = self._calculate_engagement_score(event)
                scored_events.append((event, engagement_score))

            # Sort by score and return top events
            scored_events.sort(key=lambda x: x[1], reverse=True)
            return [event for event, _ in scored_events[:limit]]

        except Exception as e:
            logger.error(f"Error getting trending recommendations: {str(e)}")
            return events[:limit]

    def _calculate_engagement_score(self, event: EventModel) -> float:
        """Calculate engagement score for trending recommendations."""
        try:
            metrics = event.metrics or {}
            
            # Combine various engagement metrics
            views = metrics.get('view_count', 0)
            saves = metrics.get('save_count', 0)
            shares = metrics.get('share_count', 0)
            registrations = metrics.get('registration_count', 0)
            
            # Weight different types of engagement
            score = (
                0.1 * views +
                0.3 * saves +
                0.3 * shares +
                0.3 * registrations
            )
            
            # Normalize score
            max_score = 1000  # Adjust based on typical engagement levels
            normalized_score = min(1.0, score / max_score)
            
            return normalized_score
            
        except Exception as e:
            logger.error(f"Error calculating engagement score: {str(e)}")
            return 0.0

    def get_similar_recommendations(
        self,
        reference_event: EventModel,
        candidate_events: List[EventModel],
        limit: int = 10
    ) -> List[EventModel]:
        """Get recommendations similar to a reference event."""
        try:
            # Extract reference event features
            ref_vectors = self.recommendation_model._extract_event_vectors(reference_event)
            
            # Score candidate events based on similarity
            scored_events = []
            for event in candidate_events:
                if event.id == reference_event.id:
                    continue
                    
                event_vectors = self.recommendation_model._extract_event_vectors(event)
                similarity_score = self._calculate_event_similarity(
                    ref_vectors, event_vectors
                )
                scored_events.append((event, similarity_score))
            
            # Sort by similarity and return top events
            scored_events.sort(key=lambda x: x[1], reverse=True)
            return [event for event, _ in scored_events[:limit]]
            
        except Exception as e:
            logger.error(f"Error getting similar recommendations: {str(e)}")
            return candidate_events[:limit]

    def _calculate_event_similarity(
        self,
        ref_vectors: Dict[str, Any],
        event_vectors: Dict[str, Any]
    ) -> float:
        """Calculate similarity between two events."""
        try:
            scores = []
            
            # Compare content vectors
            if ref_vectors.get('topic_vector') and event_vectors.get('topic_vector'):
                topic_sim = cosine_similarity(
                    [list(ref_vectors['topic_vector'].values())],
                    [list(event_vectors['topic_vector'].values())]
                )[0][0]
                scores.append(topic_sim)
            
            # Compare category vectors
            if ref_vectors.get('category_vector') and event_vectors.get('category_vector'):
                category_sim = cosine_similarity(
                    [list(ref_vectors['category_vector'].values())],
                    [list(event_vectors['category_vector'].values())]
                )[0][0]
                scores.append(category_sim)
            
            # Compare other relevant features
            if ref_vectors.get('contextual_features') and event_vectors.get('contextual_features'):
                price_diff = abs(
                    ref_vectors['contextual_features']['price_normalized'] -
                    event_vectors['contextual_features']['price_normalized']
                )
                price_sim = 1.0 - min(1.0, price_diff)
                scores.append(price_sim)
            
            return np.mean(scores) if scores else 0.0
            
        except Exception as e:
            logger.error(f"Error calculating event similarity: {str(e)}")
            return 0.0 