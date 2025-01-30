"""Recommendation engine service for event recommendations."""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta, timezone
import logging
import math
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
        
        # Add monitoring counters
        self.scoring_stats = {
            'advanced_success': 0,
            'advanced_failure': 0,
            'basic_fallback': 0,
            'total_scored': 0,
            'score_distribution': {
                '0.0-0.2': 0,
                '0.2-0.4': 0,
                '0.4-0.6': 0,
                '0.6-0.8': 0,
                '0.8-1.0': 0
            }
        }
        
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
            
            # Only apply filters if they are meaningfully provided
            if location and location.get('latitude') and location.get('longitude'):
                lat = float(location.get('latitude', 0))
                lon = float(location.get('longitude', 0))
                
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
            
            # Filter by categories if meaningfully provided
            if categories and any(categories):
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
            
            if not events:
                logger.warning("No events found matching criteria. Returning all future events.")
                # Fallback to just returning future events
                query = select(EventModel).where(
                    EventModel.start_datetime >= datetime.utcnow()
                )
                result = await self.db.execute(query)
                events = result.scalars().all()
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting candidate events: {str(e)}")
            return []
            
    async def _score_event(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate a score for an event based on user preferences and advanced ML models."""
        try:
            used_advanced = True
            used_fallback = False
            
            # Try new scoring system
            try:
                score = await self._calculate_advanced_score(event, preferences)
                
                # Validate score is reasonable
                if 0.0 <= score <= 1.0:
                    self._update_scoring_stats(score, used_advanced, used_fallback)
                    return score
                    
                logger.warning(f"Advanced scoring returned invalid score {score}, falling back to basic scoring")
                used_fallback = True
                
            except Exception as e:
                logger.error(f"Error in advanced scoring for event {event.id}, falling back to basic scoring: {str(e)}")
                used_fallback = True
            
            # Fallback to basic scoring
            score = await self._calculate_basic_score(event, preferences)
            self._update_scoring_stats(score, used_advanced, used_fallback)
            return score
            
        except Exception as e:
            logger.error(f"Critical error in score_event: {str(e)}")
            return 0.8  # Return default score as absolute last resort
            
    async def _calculate_advanced_score(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate score using advanced scoring system."""
        # Get base scores
        base_scores = {
            'location': await self._calculate_location_score(event, preferences),
            'category': self._calculate_category_score(event, preferences),
            'price': self._calculate_price_score(event, preferences),
            'time': self._calculate_time_score(event, preferences)
        }
        
        # Get user ID for advanced scoring
        if isinstance(preferences, dict):
            user_id = preferences.get("user_id")
        else:
            user_id = preferences.user_id
            
        if user_id:
            try:
                # Get collaborative filtering scores
                collab_scores = await self._get_collaborative_scores(event, user_id)
                base_scores.update(collab_scores)
                
                # Get advanced ML scores
                advanced_scores = await self._calculate_advanced_scores(event, user_id)
                base_scores.update(advanced_scores)
            except Exception as e:
                logger.warning(f"Error getting advanced scores: {str(e)}")
                # Continue with base scores only
        
        try:
            # Calculate dynamic weights based on user preferences strength
            weights = await self._calculate_dynamic_weights(preferences, base_scores)
            
            # Apply contextual boosts
            context_multipliers = await self._get_contextual_multipliers(event, preferences)
            
            # Calculate adjusted scores
            adjusted_scores = {
                key: score * context_multipliers.get(key, 1.0)
                for key, score in base_scores.items()
            }
            
            # Calculate final score with normalization
            weighted_scores = [
                score * weights[key]
                for key, score in adjusted_scores.items()
            ]
            
            # Apply ensemble learning if we have multiple scores
            if len(weighted_scores) > 1:
                try:
                    # Use stacking to combine scores
                    stacked_score = await self._stack_scores(
                        event,
                        user_id if user_id else "anonymous",
                        weighted_scores
                    )
                    
                    # Apply final calibration
                    calibrated_score = self._calibrate_score(stacked_score)
                    
                    if 0.0 <= calibrated_score <= 1.0:
                        return calibrated_score
                except Exception as e:
                    logger.warning(f"Error in ensemble scoring: {str(e)}")
                    # Fall through to simple average
            
            # Fallback to simple average if ensemble fails or not enough scores
            total_score = sum(weighted_scores)
            if total_score > 0:
                # Apply sigmoid normalization to spread scores
                normalized_score = 1 / (1 + math.exp(-2 * (total_score - 0.5)))
                return normalized_score
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error in advanced scoring calculation: {str(e)}")
            raise
            
    async def _calculate_basic_score(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate score using original basic scoring system."""
        try:
            # Calculate individual scores
            location_score = await self._calculate_location_score(event, preferences)
            category_score = self._calculate_category_score(event, preferences)
            price_score = self._calculate_price_score(event, preferences)
            time_score = self._calculate_time_score(event, preferences)
            
            # Dynamic weights based on preference strength
            weights = {
                'location': 0.35,
                'category': 0.35,
                'price': 0.15,
                'time': 0.15
            }
            
            # Calculate weighted sum
            total_score = (
                weights['location'] * location_score +
                weights['category'] * category_score +
                weights['price'] * price_score +
                weights['time'] * time_score
            )
            
            # Apply sigmoid normalization for better distribution
            normalized_score = 1 / (1 + math.exp(-4 * (total_score - 0.5)))
            
            # Apply minimum score threshold for valid events
            if total_score > 0:
                return max(0.1, min(1.0, normalized_score))
            return 0.0
            
        except Exception as e:
            logger.error(f"Error in basic scoring: {str(e)}")
            return 0.1  # Return low score on error instead of 0.15
            
    async def _calculate_emergency_score(self, event: EventModel) -> float:
        """Calculate a minimal baseline score when normal scoring fails."""
        try:
            scores = []
            
            # 1. Time-based score (future events get higher scores)
            try:
                hours_until_event = (event.start_datetime - datetime.utcnow()).total_seconds() / 3600
                if 0 <= hours_until_event <= 168:  # Next 7 days
                    scores.append(0.3)  # Moderate score for upcoming events
                elif hours_until_event > 168:
                    scores.append(0.2)  # Lower score for far future events
                else:
                    scores.append(0.0)  # Past events get zero
            except:
                pass
            
            # 2. Location presence score
            try:
                if event.venue_lat and event.venue_lon and event.venue_name:
                    scores.append(0.3)  # Moderate score for events with complete location
                elif event.venue_city and event.venue_state:
                    scores.append(0.2)  # Lower score for partial location
                else:
                    scores.append(0.1)  # Minimal score for no location
            except:
                pass
            
            # 3. Data completeness score
            try:
                completeness_score = 0.0
                if event.title:
                    completeness_score += 0.1
                if event.description:
                    completeness_score += 0.05
                if event.categories:
                    completeness_score += 0.05
                scores.append(completeness_score)
            except:
                pass
            
            # If we couldn't calculate any scores, return minimal non-zero score
            if not scores:
                return 0.1
            
            # Return average of available scores, but cap at 0.3
            # This ensures emergency scores don't compete with properly scored events
            return min(0.3, sum(scores) / len(scores))
            
        except Exception as e:
            logger.error(f"Error in emergency scoring: {str(e)}")
            return 0.1  # Absolute minimum non-zero score as last resort
            
    async def _stack_scores(self, event: EventModel, user_id: str, scores: List[float]) -> float:
        """Combine multiple scores using stacking ensemble."""
        try:
            # Get stacking weights from model
            weights = await self._get_stacking_weights(event, user_id)
            
            if not weights or len(weights) != len(scores):
                # Fallback to simple average
                return sum(scores) / len(scores)
            
            # Apply stacking weights
            stacked_score = sum(w * s for w, s in zip(weights, scores))
            
            return stacked_score
            
        except Exception as e:
            logger.error(f"Error in score stacking: {str(e)}")
            return sum(scores) / len(scores)
            
    def _calibrate_score(self, score: float) -> float:
        """Apply calibration to final score."""
        try:
            # Apply temperature scaling
            temperature = 0.1  # Lower temperature = sharper distribution
            scaled_score = 1 / (1 + math.exp(-score / temperature))
            
            # Apply histogram equalization
            # This helps spread out the scores more evenly
            if 0.4 <= scaled_score <= 0.6:
                # Spread out scores in the middle range
                spread = 0.2
                scaled_score = 0.4 + (scaled_score - 0.4) * (1 + spread)
            
            return min(1.0, max(0.0, scaled_score))
            
        except Exception as e:
            logger.error(f"Error in score calibration: {str(e)}")
            return score
            
    async def _get_collaborative_scores(self, event: EventModel, user_id: str) -> Dict[str, float]:
        """Calculate collaborative filtering scores."""
        try:
            scores = {}
            
            # User-User Collaborative Filtering
            similar_users_score = await self._calculate_user_user_similarity(event, user_id)
            scores['user_similarity'] = similar_users_score
            
            # Item-Item Collaborative Filtering
            similar_events_score = await self._calculate_item_item_similarity(event)
            scores['item_similarity'] = similar_events_score
            
            # Interaction-based scoring
            interaction_score = await self._calculate_interaction_score(event, user_id)
            scores['interaction'] = interaction_score
            
            return scores
            
        except Exception as e:
            logger.error(f"Error calculating collaborative scores: {str(e)}")
            return {
                'user_similarity': 0.5,
                'item_similarity': 0.5,
                'interaction': 0.5
            }
            
    async def _calculate_user_user_similarity(self, event: EventModel, user_id: str) -> float:
        """Calculate similarity score based on similar users' preferences."""
        try:
            # Get users who interacted with similar events
            query = select(EventModel.user_interactions).where(
                EventModel.categories.overlap(event.categories)
            )
            result = await self.db.execute(query)
            similar_users = result.scalars().all()
            
            if not similar_users:
                return 0.5
                
            # Calculate similarity scores
            similarity_scores = []
            for other_user in similar_users:
                # Compare user traits and preferences
                trait_similarity = await self._calculate_trait_similarity(user_id, other_user)
                pref_similarity = await self._calculate_preference_similarity(user_id, other_user)
                
                similarity_scores.append(0.6 * trait_similarity + 0.4 * pref_similarity)
            
            # Weight by interaction recency and frequency
            weighted_score = await self._apply_temporal_weights(similarity_scores, similar_users)
            
            return weighted_score
            
        except Exception as e:
            logger.error(f"Error calculating user-user similarity: {str(e)}")
            return 0.5
            
    async def _calculate_item_item_similarity(self, event: EventModel) -> float:
        """Calculate similarity score based on similar events."""
        try:
            # Get similar events based on multiple factors
            similar_events = await self._find_similar_events(event)
            
            if not similar_events:
                return 0.5
                
            # Calculate content-based similarity
            content_scores = []
            for similar_event in similar_events:
                # Compare event attributes
                title_sim = calculate_title_similarity(event.title, similar_event.title)
                category_sim = len(set(event.categories) & set(similar_event.categories)) / \
                             len(set(event.categories) | set(similar_event.categories)) if event.categories and similar_event.categories else 0
                venue_sim = 1.0 if event.venue_name == similar_event.venue_name else 0.0
                
                # Combine similarities with weights
                similarity = (0.4 * title_sim + 0.4 * category_sim + 0.2 * venue_sim)
                content_scores.append(similarity)
            
            # Calculate popularity and engagement metrics
            popularity_score = await self._calculate_popularity_score(similar_events)
            
            # Combine scores with weights
            return 0.7 * (sum(content_scores) / len(content_scores)) + 0.3 * popularity_score
            
        except Exception as e:
            logger.error(f"Error calculating item-item similarity: {str(e)}")
            return 0.5
            
    async def _calculate_interaction_score(self, event: EventModel, user_id: str) -> float:
        """Calculate score based on user interactions."""
        try:
            # Get user's interaction history
            query = select(EventModel).join(
                EventModel.user_interactions
            ).where(
                EventModel.user_interactions.any(user_id=user_id)
            )
            result = await self.db.execute(query)
            interaction_history = result.scalars().all()
            
            if not interaction_history:
                return 0.5
                
            # Calculate interaction patterns
            view_score = await self._calculate_view_pattern_score(interaction_history, event)
            attendance_score = await self._calculate_attendance_pattern_score(interaction_history, event)
            category_preference_score = await self._calculate_category_preference_score(interaction_history, event)
            
            # Weight different interaction types
            return (
                0.3 * view_score +
                0.4 * attendance_score +
                0.3 * category_preference_score
            )
            
        except Exception as e:
            logger.error(f"Error calculating interaction score: {str(e)}")
            return 0.5

    async def _calculate_dynamic_weights(
        self,
        preferences: Union[UserPreferencesBase, Dict[str, Any]],
        base_scores: Dict[str, float]
    ) -> Dict[str, float]:
        """Calculate dynamic weights based on user preferences strength."""
        try:
            weights = {
                'location': 0.35,  # Increased base weight for location
                'category': 0.35,  # Increased base weight for category
                'price': 0.15,    # Reduced base weight for price
                'time': 0.15      # Reduced base weight for time
            }
            
            if isinstance(preferences, dict):
                # Adjust location weight based on if user has location preference
                if preferences.get("preferred_location"):
                    weights['location'] *= 1.4  # Increased multiplier
                
                # Adjust category weight based on number of preferred categories
                if categories := preferences.get("preferred_categories", []):
                    weights['category'] *= min(1.8, 1 + len(categories) / 8)  # More aggressive scaling
                
                # Adjust price weight if max price specified
                if preferences.get("max_price") is not None:
                    weights['price'] *= 1.3  # Increased multiplier
                    
                # Adjust time weight based on time preferences
                if preferences.get("preferred_times") or preferences.get("preferred_days"):
                    weights['time'] *= 1.4  # Increased multiplier
            else:
                # Similar adjustments for UserPreferencesBase object
                if preferences.preferred_location:
                    weights['location'] *= 1.4
                    
                if preferences.preferred_categories:
                    weights['category'] *= min(1.8, 1 + len(preferences.preferred_categories) / 8)
                    
                if preferences.max_price is not None:
                    weights['price'] *= 1.3
                    
                if preferences.preferred_times or preferences.preferred_days:
                    weights['time'] *= 1.4
            
            # Apply score-based adjustments
            for key, score in base_scores.items():
                if score > 0.8:  # Boost weights for high base scores
                    weights[key] *= 1.2
                elif score < 0.3:  # Reduce weights for low base scores
                    weights[key] *= 0.8
            
            # Normalize weights to sum to 1
            total = sum(weights.values())
            normalized_weights = {k: v/total for k, v in weights.items()}
            
            # Apply sigmoid normalization to create more spread
            import math
            for key in normalized_weights:
                x = normalized_weights[key] * 10 - 5  # Scale to -5 to 5 range
                normalized_weights[key] = 1 / (1 + math.exp(-x))  # Sigmoid
            
            # Final normalization
            total = sum(normalized_weights.values())
            return {k: v/total for k, v in normalized_weights.items()}
            
        except Exception as e:
            logger.error(f"Error calculating dynamic weights: {str(e)}")
            return {
                'location': 0.25,
                'category': 0.25,
                'price': 0.25,
                'time': 0.25
            }

    async def _get_contextual_multipliers(
        self,
        event: EventModel,
        preferences: Union[UserPreferencesBase, Dict[str, Any]]
    ) -> Dict[str, float]:
        """Calculate contextual score multipliers with bandit learning."""
        try:
            # Get base multipliers
            multipliers = {
                'location': 1.0,
                'category': 1.0,
                'price': 1.0,
                'time': 1.0
            }
            
            # Get user ID for contextual learning
            if isinstance(preferences, dict):
                user_id = preferences.get("user_id")
            else:
                user_id = preferences.user_id
            
            # Get context features
            context = await self._get_context_features(event, preferences)
            
            # Apply contextual bandit learning
            if user_id:
                bandit_multipliers = await self._get_bandit_multipliers(
                    user_id,
                    event,
                    context
                )
                multipliers.update(bandit_multipliers)
            
            # Apply time-based boosts - Fix datetime handling
            current_time = datetime.now(timezone.utc)
            event_time = event.start_datetime
            if not event_time.tzinfo:
                event_time = event_time.replace(tzinfo=timezone.utc)
            
            hours_until_event = (event_time - current_time).total_seconds() / 3600
            if 0 <= hours_until_event <= 48:
                multipliers['time'] *= 1.2
            
            # Apply price boosts
            if not event.price_info or event.price_info == "null" or event.price_info == []:
                multipliers['price'] *= 1.15
            
            # Apply location quality boosts
            if event.venue_lat and event.venue_lon and event.venue_name:
                multipliers['location'] *= 1.1
            
            # Apply category match boosts
            if isinstance(preferences, dict):
                preferred_cats = preferences.get("preferred_categories", [])
            else:
                preferred_cats = preferences.preferred_categories or []
                
            if preferred_cats and event.categories:
                matching_cats = len(set(event.categories) & set(preferred_cats))
                if matching_cats > 1:
                    multipliers['category'] *= min(1.3, 1 + (matching_cats - 1) * 0.1)
            
            return multipliers
            
        except Exception as e:
            logger.error(f"Error calculating contextual multipliers: {str(e)}")
            return {
                'location': 1.0,
                'category': 1.0,
                'price': 1.0,
                'time': 1.0
            }
            
    async def _get_context_features(
        self,
        event: EventModel,
        preferences: Union[UserPreferencesBase, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Extract context features for bandit learning."""
        try:
            # Time context
            current_time = datetime.utcnow().replace(tzinfo=timezone.utc)
            time_features = {
                'hour': current_time.hour,
                'day': current_time.weekday(),
                'month': current_time.month,
                'is_weekend': current_time.weekday() >= 5,
                'days_until_event': (event.start_datetime - current_time).days
            }
            
            # User context
            user_features = {}
            if isinstance(preferences, dict):
                user_id = preferences.get("user_id")
                if user_id:
                    user_features = await self._get_user_context(user_id)
            
            # Event context
            event_features = {
                'has_price': bool(event.price_info),
                'has_location': bool(event.venue_lat and event.venue_lon),
                'category_count': len(event.categories) if event.categories else 0,
                'is_popular': await self._check_event_popularity(event)
            }
            
            # Location context
            location_features = await self._get_location_context(event)
            
            return {
                'time': time_features,
                'user': user_features,
                'event': event_features,
                'location': location_features
            }
            
        except Exception as e:
            logger.error(f"Error getting context features: {str(e)}")
            return {}
            
    async def _get_bandit_multipliers(
        self,
        user_id: str,
        event: EventModel,
        context: Dict[str, Any]
    ) -> Dict[str, float]:
        """Get multipliers from contextual bandit model."""
        try:
            # Get bandit arm features
            arms = {
                'location': self._get_location_arm_features(context),
                'category': self._get_category_arm_features(context),
                'price': self._get_price_arm_features(context),
                'time': self._get_time_arm_features(context)
            }
            
            # Get historical rewards
            rewards = await self._get_historical_rewards(user_id)
            
            # Apply Thompson Sampling
            multipliers = {}
            for arm_name, arm_features in arms.items():
                # Get beta distribution parameters from historical rewards
                alpha, beta = self._get_beta_parameters(rewards, arm_name)
                
                # Sample from beta distribution
                from numpy.random import beta as beta_random
                multiplier = beta_random(alpha, beta)
                
                # Apply exploration bonus for arms with less data
                exploration_bonus = self._calculate_exploration_bonus(rewards, arm_name)
                multiplier *= (1 + exploration_bonus)
                
                multipliers[arm_name] = multiplier
            
            # Normalize multipliers
            total = sum(multipliers.values())
            return {k: v/total for k, v in multipliers.items()}
            
        except Exception as e:
            logger.error(f"Error getting bandit multipliers: {str(e)}")
            return {
                'location': 0.25,
                'category': 0.25,
                'price': 0.25,
                'time': 0.25
            }
            
    def _calculate_exploration_bonus(
        self,
        rewards: Dict[str, List[float]],
        arm_name: str
    ) -> float:
        """Calculate UCB-style exploration bonus."""
        try:
            # Get number of pulls for this arm
            n_pulls = len(rewards.get(arm_name, []))
            
            if n_pulls == 0:
                return 0.5  # High exploration bonus for never-pulled arms
            
            # Calculate UCB1 exploration term
            from math import log, sqrt
            total_pulls = sum(len(pulls) for pulls in rewards.values())
            exploration_term = sqrt(2 * log(total_pulls) / n_pulls)
            
            # Normalize to 0-0.5 range
            return min(0.5, exploration_term / 4)
            
        except Exception as e:
            logger.error(f"Error calculating exploration bonus: {str(e)}")
            return 0.1
            
    def _calculate_category_score(self, event: EventModel, preferences: Union[UserPreferencesBase, Dict[str, Any]]) -> float:
        """Calculate category preference score with semantic matching."""
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
            if excluded_categories and event.categories:
                if any(self._categories_match(cat, excluded) 
                      for cat in event.categories 
                      for excluded in excluded_categories):
                    return 0.0
            
            if not event.categories:
                return 0.1  # Small non-zero score for events with no categories
            
            # Calculate semantic matching scores
            match_scores = []
            for preferred in preferred_categories:
                # Get best match score for this preferred category
                best_match = max(
                    (self._categories_match(preferred, event_cat)
                     for event_cat in event.categories),
                    default=0.0
                )
                match_scores.append(best_match)
            
            # Calculate final score based on best matches
            if not match_scores:
                return 0.1
            
            # Use both average and maximum match scores
            avg_score = sum(match_scores) / len(match_scores)
            max_score = max(match_scores)
            
            # Combine scores with emphasis on best match
            return 0.7 * max_score + 0.3 * avg_score
            
        except Exception as e:
            logger.error(f"Error calculating category score: {str(e)}")
            return 0.0
            
    def _categories_match(self, cat1: str, cat2: str) -> float:
        """Calculate semantic match score between two categories."""
        try:
            # Normalize categories
            cat1 = cat1.lower().strip()
            cat2 = cat2.lower().strip()
            
            # Exact match
            if cat1 == cat2:
                return 1.0
                
            # Handle common variations
            cat1_words = set(cat1.split())
            cat2_words = set(cat2.split())
            
            # Calculate word overlap
            common_words = cat1_words & cat2_words
            total_words = cat1_words | cat2_words
            
            if not total_words:
                return 0.0
                
            # Calculate Jaccard similarity
            similarity = len(common_words) / len(total_words)
            
            # Boost score for partial matches
            if similarity > 0:
                # Check if one is substring of other
                if cat1 in cat2 or cat2 in cat1:
                    similarity = min(1.0, similarity + 0.3)
                    
                # Check for common category patterns
                common_patterns = [
                    ('music', 'concert'),
                    ('art', 'exhibition'),
                    ('food', 'dining'),
                    ('sports', 'athletic'),
                    ('business', 'networking'),
                    ('education', 'learning'),
                    ('family', 'kids'),
                ]
                
                for pattern1, pattern2 in common_patterns:
                    if ((pattern1 in cat1 and pattern2 in cat2) or
                        (pattern2 in cat1 and pattern1 in cat2)):
                        similarity = min(1.0, similarity + 0.2)
            
            return similarity
            
        except Exception as e:
            logger.error(f"Error matching categories: {str(e)}")
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
        """Calculate time preference score with temporal relevance."""
        try:
            if isinstance(preferences, dict):
                preferred_days = preferences.get("preferred_days", [])
                preferred_times = preferences.get("preferred_times", [])
            else:
                preferred_days = preferences.preferred_days or []
                preferred_times = preferences.preferred_times or []
            
            # Calculate base temporal scores
            day_score = self._calculate_day_match(event, preferred_days)
            time_score = self._calculate_time_match(event, preferred_times)
            scheduling_score = self._calculate_scheduling_score(event)
            
            # If no preferences specified, use scheduling score with higher weight
            if not (preferred_days or preferred_times):
                return 0.3 * day_score + 0.3 * time_score + 0.4 * scheduling_score
            
            # Calculate weighted score based on preference strength
            weights = self._calculate_time_weights(preferred_days, preferred_times)
            
            final_score = (
                weights['day'] * day_score +
                weights['time'] * time_score +
                weights['scheduling'] * scheduling_score
            )
            
            return min(1.0, max(0.0, final_score))
            
        except Exception as e:
            logger.error(f"Error calculating time score: {str(e)}")
            return 0.0
            
    def _calculate_day_match(self, event: EventModel, preferred_days: List[str]) -> float:
        """Calculate day preference match score."""
        try:
            if not preferred_days:
                return 0.5  # Neutral score if no preferences
                
                event_day = event.start_datetime.strftime("%A").lower()
            
            # Check for exact day match
            if event_day in [day.lower() for day in preferred_days]:
                return 1.0
                
            # Calculate score based on day proximity
            day_indices = {
                'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
                'friday': 4, 'saturday': 5, 'sunday': 6
            }
            
            event_idx = day_indices[event_day]
            preferred_indices = [day_indices[day.lower()] for day in preferred_days]
            
            # Calculate minimum distance to any preferred day
            min_distance = min(
                min(abs(event_idx - pref_idx), 7 - abs(event_idx - pref_idx))
                for pref_idx in preferred_indices
            )
            
            # Score decreases with distance from preferred days
            return max(0.0, 1.0 - (min_distance * 0.2))
            
        except Exception as e:
            logger.error(f"Error calculating day match: {str(e)}")
            return 0.0
            
    def _calculate_time_match(self, event: EventModel, preferred_times: List[str]) -> float:
        """Calculate time preference match score."""
        try:
            if not preferred_times:
                return 0.5  # Neutral score if no preferences
                
                event_hour = event.start_datetime.hour
            
            # Define time ranges with fuzzy boundaries
                time_ranges = {
                "morning": {
                    "core": (7, 11),    # Core morning hours
                    "extended": (5, 12)  # Extended morning hours
                },
                "afternoon": {
                    "core": (12, 16),
                    "extended": (11, 17)
                },
                "evening": {
                    "core": (17, 21),
                    "extended": (16, 22)
                },
                "night": {
                    "core": (21, 23),
                    "extended": (20, 5)
                }
            }
            
            max_score = 0.0
            for preferred in preferred_times:
                preferred = preferred.lower()
                if preferred not in time_ranges:
                    continue
                    
                ranges = time_ranges[preferred]
                core_start, core_end = ranges["core"]
                ext_start, ext_end = ranges["extended"]
                
                # Check if event is in core hours
                if self._is_hour_in_range(event_hour, core_start, core_end):
                    max_score = max(max_score, 1.0)
                # Check if event is in extended hours
                elif self._is_hour_in_range(event_hour, ext_start, ext_end):
                    # Calculate score based on distance to core hours
                    distance_to_core = min(
                        abs(event_hour - core_start),
                        abs(event_hour - core_end)
                    )
                    max_score = max(max_score, 1.0 - (distance_to_core * 0.2))
            
            return max_score
            
        except Exception as e:
            logger.error(f"Error calculating time match: {str(e)}")
            return 0.0
            
    def _calculate_scheduling_score(self, event: EventModel) -> float:
        """Calculate scheduling quality score."""
        try:
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            event_start = event.start_datetime
            if event_start.tzinfo is None:
                event_start = event_start.replace(tzinfo=timezone.utc)
            
            hours_until_event = (event_start - now).total_seconds() / 3600
            
            # Scoring factors:
            scores = []
            
            # 1. Notice period (higher score for events with reasonable notice)
            if 24 <= hours_until_event <= 168:  # 1-7 days notice
                scores.append(1.0)
            elif hours_until_event < 24:  # Less than 1 day notice
                scores.append(0.5)
            elif hours_until_event <= 336:  # 7-14 days notice
                scores.append(0.8)
            else:  # More than 14 days notice
                scores.append(0.6)
            
            # 2. Duration score (if end time available)
            if event.end_datetime:
                event_end = event.end_datetime
                if event_end.tzinfo is None:
                    event_end = event_end.replace(tzinfo=timezone.utc)
                duration_hours = (event_end - event_start).total_seconds() / 3600
                if 1 <= duration_hours <= 4:  # Ideal duration
                    scores.append(1.0)
                elif duration_hours < 1:  # Too short
                    scores.append(0.6)
                elif duration_hours <= 8:  # Long but acceptable
                    scores.append(0.8)
                else:  # Very long
                    scores.append(0.4)
            
            # 3. Time of day score
            hour = event_start.hour
            if 10 <= hour <= 20:  # Prime hours
                scores.append(1.0)
            elif 7 <= hour < 10 or 20 < hour <= 22:  # Shoulder hours
                scores.append(0.8)
            else:  # Off hours
                scores.append(0.5)
            
            return sum(scores) / len(scores) if scores else 0.5
            
        except Exception as e:
            logger.error(f"Error calculating scheduling score: {str(e)}")
            return 0.5
            
    def _calculate_time_weights(self, preferred_days: List[str], preferred_times: List[str]) -> Dict[str, float]:
        """Calculate weights for different time factors based on preference strength."""
        try:
            weights = {'day': 0.3, 'time': 0.3, 'scheduling': 0.4}
            
            # Adjust weights based on preference strength
            if preferred_days and not preferred_times:
                weights['day'] = 0.5
                weights['time'] = 0.2
                weights['scheduling'] = 0.3
            elif preferred_times and not preferred_days:
                weights['day'] = 0.2
                weights['time'] = 0.5
                weights['scheduling'] = 0.3
            elif preferred_days and preferred_times:
                weights['day'] = 0.35
                weights['time'] = 0.35
                weights['scheduling'] = 0.3
            
            return weights
            
        except Exception as e:
            logger.error(f"Error calculating time weights: {str(e)}")
            return {'day': 0.33, 'time': 0.33, 'scheduling': 0.34}
            
    def _is_hour_in_range(self, hour: int, start: int, end: int) -> bool:
        """Check if hour falls within range, handling overnight ranges."""
        if start <= end:
            return start <= hour < end
        else:  # Range spans midnight
            return hour >= start or hour < end
            
    async def _calculate_location_score(
        self,
        event: EventModel,
        preferences: Union[UserPreferencesBase, Dict[str, Any]]
    ) -> float:
        """Calculate location preference score with neighborhood and accessibility factors."""
        try:
            # Get user location preferences
            if isinstance(preferences, dict):
                if not preferences.get("preferred_location"):
                    return 0.5  # Neutral score instead of 1.0 for no preference
                user_lat = float(preferences["preferred_location"].get("latitude", 0))
                user_lon = float(preferences["preferred_location"].get("longitude", 0))
                max_distance = preferences.get("max_distance", self.default_max_distance)
            else:
                if not preferences.preferred_location:
                    return 0.5  # Neutral score instead of 1.0 for no preference
                user_lat = float(preferences.preferred_location.latitude or 0)
                user_lon = float(preferences.preferred_location.longitude or 0)
                max_distance = preferences.max_distance or self.default_max_distance
            
            if not (event.venue_lat and event.venue_lon):
                return 0.1  # Small non-zero score for events with no location
            
            # Calculate base distance score using inverse exponential decay
            distance = self._calculate_distance(
                user_lat,
                user_lon,
                event.venue_lat,
                event.venue_lon
            )
            
            # Use exponential decay for smoother distance scoring
            distance_score = math.exp(-distance / (max_distance / 2))
            
            # Calculate venue completeness score
            venue_info_score = self._calculate_venue_completeness(event)
            
            # Calculate accessibility score
            accessibility_score = self._calculate_accessibility_score(event)
            
            # Combine scores with weights
            final_score = (
                0.6 * distance_score +      # Distance is most important
                0.2 * venue_info_score +    # Venue information quality
                0.2 * accessibility_score    # Accessibility factors
            )
            
            return min(1.0, max(0.0, final_score))
            
        except Exception as e:
            logger.error(f"Error calculating location score: {str(e)}")
            return 0.0
            
    def _calculate_venue_completeness(self, event: EventModel) -> float:
        """Calculate score based on venue information completeness."""
        try:
            # Define required venue fields
            venue_fields = [
                (event.venue_name, 0.3),
                (event.venue_city, 0.2),
                (event.venue_state, 0.2),
                (event.venue_country, 0.1),
                ((event.venue_lat, event.venue_lon), 0.2)
            ]
            
            # Calculate weighted completeness score
            score = sum(
                weight
                for field, weight in venue_fields
                if (isinstance(field, tuple) and all(f for f in field)) or
                   (not isinstance(field, tuple) and field)
            )
            
            return score
            
        except Exception as e:
            logger.error(f"Error calculating venue completeness: {str(e)}")
            return 0.0
            
    def _calculate_accessibility_score(self, event: EventModel) -> float:
        """Calculate venue accessibility score."""
        try:
            base_score = 0.5  # Start with neutral score
            
            # Boost score for venues in well-known areas
            if event.venue_city and event.venue_state:
                # List of major cities/areas that likely have good accessibility
                major_areas = {
                    ("Louisville", "KY"): 0.3,
                    ("New Albany", "IN"): 0.2,
                    ("Jeffersonville", "IN"): 0.2,
                    ("Clarksville", "IN"): 0.2
                }
                
                area_key = (event.venue_city.strip(), event.venue_state.strip())
                if area_key in major_areas:
                    base_score += major_areas[area_key]
            
            # Boost score for venues with complete address info
            if all([event.venue_name, event.venue_city, event.venue_state]):
                base_score += 0.2
            
            return min(1.0, base_score)
            
        except Exception as e:
            logger.error(f"Error calculating accessibility score: {str(e)}")
            return 0.5
    
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
            
    def _update_scoring_stats(self, score: float, used_advanced: bool, used_fallback: bool) -> None:
        """Update scoring statistics."""
        try:
            self.scoring_stats['total_scored'] += 1
            
            if used_advanced and not used_fallback:
                self.scoring_stats['advanced_success'] += 1
            elif used_advanced and used_fallback:
                self.scoring_stats['advanced_failure'] += 1
            
            if used_fallback:
                self.scoring_stats['basic_fallback'] += 1
            
            # Update score distribution
            if 0.0 <= score <= 0.2:
                self.scoring_stats['score_distribution']['0.0-0.2'] += 1
            elif score <= 0.4:
                self.scoring_stats['score_distribution']['0.2-0.4'] += 1
            elif score <= 0.6:
                self.scoring_stats['score_distribution']['0.4-0.6'] += 1
            elif score <= 0.8:
                self.scoring_stats['score_distribution']['0.6-0.8'] += 1
            else:
                self.scoring_stats['score_distribution']['0.8-1.0'] += 1
                
            # Log stats periodically
            if self.scoring_stats['total_scored'] % 100 == 0:
                self._log_scoring_stats()
                
        except Exception as e:
            logger.error(f"Error updating scoring stats: {str(e)}")
            
    def _log_scoring_stats(self) -> None:
        """Log current scoring statistics."""
        try:
            total = self.scoring_stats['total_scored']
            if total == 0:
                return
                
            advanced_success_rate = self.scoring_stats['advanced_success'] / total * 100
            fallback_rate = self.scoring_stats['basic_fallback'] / total * 100
            
            logger.info(f"\nScoring System Statistics:")
            logger.info(f"Total Events Scored: {total}")
            logger.info(f"Advanced Scoring Success Rate: {advanced_success_rate:.1f}%")
            logger.info(f"Fallback Rate: {fallback_rate:.1f}%")
            logger.info("Score Distribution:")
            for range_key, count in self.scoring_stats['score_distribution'].items():
                percentage = count / total * 100
                logger.info(f"  {range_key}: {percentage:.1f}%")
                
        except Exception as e:
            logger.error(f"Error logging scoring stats: {str(e)}")

    async def _get_stacking_weights(self, event: EventModel, user_id: str) -> List[float]:
        """Get weights for stacking different scores."""
        try:
            # Default weights that sum to 1.0
            default_weights = [0.25, 0.25, 0.25, 0.25]
            
            # In a production system, these weights would be learned from user interactions
            # For now, we use fixed weights that emphasize score diversity
            weights = [
                0.3,  # Base score weight
                0.3,  # Collaborative score weight
                0.2,  # Time-based score weight
                0.2   # Context score weight
            ]
            
            return weights
            
        except Exception as e:
            logger.error(f"Error getting stacking weights: {str(e)}")
            return default_weights

    async def _check_event_popularity(self, event: EventModel) -> bool:
        """Check if an event is considered popular based on various signals."""
        try:
            # Consider an event popular if it has significant interactions or matches certain criteria
            popularity_signals = 0
            
            # Check if event has a meaningful description
            if event.description and len(event.description) > 100:
                popularity_signals += 1
                
            # Check if event has complete venue information
            if event.venue_name and event.venue_lat and event.venue_lon:
                popularity_signals += 1
                
            # Check if event has categories
            if event.categories and len(event.categories) > 0:
                popularity_signals += 1
                
            # Check if event has a URL
            if event.url:
                popularity_signals += 1
                
            # Consider popular if it has most signals
            return popularity_signals >= 3
            
        except Exception as e:
            logger.error(f"Error checking event popularity: {str(e)}")
            return False

    async def _get_location_context(self, event: EventModel) -> Dict[str, Any]:
        """Get location context features for an event."""
        try:
            context = {
                'has_coordinates': bool(event.venue_lat and event.venue_lon),
                'has_venue_name': bool(event.venue_name),
                'has_city': bool(event.venue_city),
                'has_state': bool(event.venue_state),
                'has_country': bool(event.venue_country)
            }
            
            # Add location quality score
            location_quality = 0
            if context['has_coordinates']:
                location_quality += 0.4
            if context['has_venue_name']:
                location_quality += 0.3
            if context['has_city']:
                location_quality += 0.2
            if context['has_state']:
                location_quality += 0.1
                
            context['location_quality'] = location_quality
            
            return context
            
        except Exception as e:
            logger.error(f"Error getting location context: {str(e)}")
            return {
                'has_coordinates': False,
                'has_venue_name': False,
                'has_city': False,
                'has_state': False,
                'has_country': False,
                'location_quality': 0.0
            } 