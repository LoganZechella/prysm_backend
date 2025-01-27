"""Feature vectorizer for combining and normalizing features."""
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
from datetime import datetime
import logging

from .base import BaseFeatureProcessor
from .user_traits import UserTraitProcessor
from .event_traits import EventFeatureProcessor

logger = logging.getLogger(__name__)

class FeatureVectorizer(BaseFeatureProcessor):
    """Combines and normalizes features for recommendation processing."""
    
    def __init__(
        self,
        user_processor: UserTraitProcessor,
        event_processor: EventFeatureProcessor
    ):
        """Initialize the feature vectorizer.
        
        Args:
            user_processor: Processor for user features
            event_processor: Processor for event features
        """
        super().__init__()
        self.user_processor = user_processor
        self.event_processor = event_processor
        
        # Define combined feature space
        self._feature_names = [
            # User-Event content matching features
            "genre_match_score",
            "topic_match_score",
            "professional_match_score",
            
            # Temporal compatibility features
            "time_compatibility",
            "schedule_match_score",
            
            # Social compatibility features
            "social_alignment_score",
            "engagement_match_score",
            
            # Contextual features
            "price_compatibility",
            "location_score",
            "capacity_preference_match"
        ]
        
        # Feature importance weights (to be tuned)
        self._feature_importance = {
            "genre_match_score": 0.15,
            "topic_match_score": 0.15,
            "professional_match_score": 0.12,
            "time_compatibility": 0.10,
            "schedule_match_score": 0.08,
            "social_alignment_score": 0.10,
            "engagement_match_score": 0.08,
            "price_compatibility": 0.08,
            "location_score": 0.07,
            "capacity_preference_match": 0.07
        }
    
    async def process(
        self,
        user_features: Dict[str, Any],
        event_features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process and combine user and event features.
        
        Args:
            user_features: Processed user features
            event_features: Processed event features
            
        Returns:
            Dictionary containing combined features
        """
        try:
            with self.performance_monitor.monitor_operation('combine_features'):
                features = {}
                
                # Calculate content matching scores
                features.update(await self._calculate_content_matches(
                    user_features,
                    event_features
                ))
                
                # Calculate temporal compatibility
                features.update(self._calculate_temporal_compatibility(
                    user_features,
                    event_features
                ))
                
                # Calculate social compatibility
                features.update(self._calculate_social_compatibility(
                    user_features,
                    event_features
                ))
                
                # Calculate contextual features
                features.update(self._calculate_contextual_features(
                    user_features,
                    event_features
                ))
                
                self.last_processed = datetime.utcnow()
                return features
                
        except Exception as e:
            logger.error(f"Error combining features: {str(e)}")
            raise
    
    async def validate(self, features: Dict[str, Any]) -> bool:
        """Validate combined features.
        
        Args:
            features: Combined features to validate
            
        Returns:
            True if features are valid, False otherwise
        """
        try:
            # Check required features
            if not self._validate_features(features):
                return False
            
            # All features should be normalized to [0,1]
            for feature_name in self._feature_names:
                if feature_name in features:
                    value = features[feature_name]
                    if not isinstance(value, (int, float)) or not (0 <= value <= 1):
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating combined features: {str(e)}")
            return False
    
    async def _calculate_content_matches(
        self,
        user_features: Dict[str, Any],
        event_features: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate content-based matching scores."""
        features = {}
        
        # Calculate genre match
        if 'genre_vector' in user_features and 'category_vector' in event_features:
            features['genre_match_score'] = self._calculate_vector_similarity(
                user_features['genre_vector'],
                event_features['category_vector']
            )
        
        # Calculate topic match
        if 'professional_interests' in user_features and 'topic_vector' in event_features:
            # TODO: Implement proper topic matching
            features['topic_match_score'] = 0.5  # Placeholder
        
        # Calculate professional interest match
        if 'skill_vector' in user_features and 'description_embedding' in event_features:
            features['professional_match_score'] = self._calculate_vector_similarity(
                user_features['skill_vector'],
                event_features['description_embedding']
            )
        
        return features
    
    def _calculate_temporal_compatibility(
        self,
        user_features: Dict[str, Any],
        event_features: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate temporal compatibility scores."""
        features = {}
        
        # Calculate time of day compatibility
        if 'activity_times' in user_features and 'time_of_day' in event_features:
            event_hour = str(event_features['time_of_day'])
            features['time_compatibility'] = user_features['activity_times'].get(event_hour, 0.0)
        
        # Calculate schedule match (placeholder for now)
        features['schedule_match_score'] = 0.5
        
        return features
    
    def _calculate_social_compatibility(
        self,
        user_features: Dict[str, Any],
        event_features: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate social compatibility scores."""
        features = {}
        
        # Calculate social alignment
        if all(key in user_features for key in ['social_engagement', 'collaboration_score']):
            user_social_score = (
                user_features['social_engagement'] +
                user_features['collaboration_score']
            ) / 2
            
            event_social_score = event_features.get('social_score', 0.5)
            features['social_alignment_score'] = 1 - abs(user_social_score - event_social_score)
        
        # Calculate engagement match
        if 'engagement_level' in user_features:
            features['engagement_match_score'] = 1 - abs(
                user_features['engagement_level'] -
                event_features.get('attendance_score', 0.5)
            )
        
        return features
    
    def _calculate_contextual_features(
        self,
        user_features: Dict[str, Any],
        event_features: Dict[str, Any]
    ) -> Dict[str, float]:
        """Calculate contextual feature scores."""
        features = {}
        
        # Price compatibility (placeholder - to be implemented based on user preferences)
        features['price_compatibility'] = 1 - event_features.get('price_normalized', 0.0)
        
        # Location score (placeholder - to be implemented with proper location matching)
        features['location_score'] = 0.5
        
        # Capacity preference (placeholder - to be implemented based on user preferences)
        features['capacity_preference_match'] = event_features.get('capacity_normalized', 0.5)
        
        return features
    
    def _calculate_vector_similarity(
        self,
        vec1: np.ndarray,
        vec2: np.ndarray
    ) -> float:
        """Calculate cosine similarity between two vectors."""
        if vec1.shape != vec2.shape:
            return 0.0
            
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
            
        return float(np.dot(vec1, vec2) / (norm1 * norm2))
    
    def calculate_match_score(self, features: Dict[str, float]) -> float:
        """Calculate overall match score from combined features.
        
        Args:
            features: Combined feature dictionary
            
        Returns:
            Float between 0 and 1 representing overall match score
        """
        score = 0.0
        total_weight = 0.0
        
        for feature_name, importance in self._feature_importance.items():
            if feature_name in features:
                score += features[feature_name] * importance
                total_weight += importance
        
        if total_weight == 0:
            return 0.0
            
        return score / total_weight 