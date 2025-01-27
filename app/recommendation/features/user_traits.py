"""Processor for extracting features from user traits."""
from typing import Dict, Any, List, Set
from datetime import datetime
import numpy as np
from collections import Counter
import logging

from .base import BaseFeatureProcessor
from app.services.trait_extractor import TraitExtractor
from app.models.traits import Traits
from app.recommendation.models.embeddings import TextEmbedding, CategoryEmbedding

logger = logging.getLogger(__name__)

class UserTraitProcessor(BaseFeatureProcessor):
    """Processor for user trait features."""
    
    def __init__(
        self,
        trait_extractor: TraitExtractor,
        text_embedding: TextEmbedding,
        category_embedding: CategoryEmbedding
    ):
        """Initialize the user trait processor.
        
        Args:
            trait_extractor: TraitExtractor instance for accessing user traits
            text_embedding: Text embedding model
            category_embedding: Category embedding model
        """
        super().__init__()
        self.trait_extractor = trait_extractor
        self.text_embedding = text_embedding
        self.category_embedding = category_embedding
        
        # Define feature categories
        self._feature_names = [
            # Music preferences
            "genre_vector",
            "artist_diversity",
            "music_recency",
            "music_consistency",
            
            # Social traits
            "social_engagement",
            "collaboration_score",
            "discovery_score",
            
            # Professional traits
            "professional_interests",
            "skill_vector",
            "industry_vector",
            
            # Behavioral traits
            "activity_times",
            "engagement_level",
            "exploration_score"
        ]
        
    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process user traits into feature vectors.
        
        Args:
            data: Raw user trait data
            
        Returns:
            Dictionary containing processed features
        """
        try:
            with self.performance_monitor.monitor_operation('process_user_traits'):
                features = {}
                
                # Process music traits
                if music_traits := data.get('music_traits'):
                    features.update(await self._process_music_traits(music_traits))
                
                # Process social traits
                if social_traits := data.get('social_traits'):
                    features.update(await self._process_social_traits(social_traits))
                
                # Process professional traits
                if professional_traits := data.get('professional_traits'):
                    features.update(await self._process_professional_traits(professional_traits))
                
                # Process behavioral traits
                if behavior_traits := data.get('behavior_traits'):
                    features.update(await self._process_behavior_traits(behavior_traits))
                
                self.last_processed = datetime.utcnow()
                return features
                
        except Exception as e:
            logger.error(f"Error processing user traits: {str(e)}")
            raise
    
    async def validate(self, features: Dict[str, Any]) -> bool:
        """Validate processed user features.
        
        Args:
            features: Processed features to validate
            
        Returns:
            True if features are valid, False otherwise
        """
        try:
            # Check required features
            if not self._validate_features(features):
                return False
            
            # Validate vector dimensions
            for key in ['genre_vector', 'skill_vector', 'industry_vector']:
                if key in features and not isinstance(features[key], np.ndarray):
                    return False
            
            # Validate score ranges
            score_features = [
                'artist_diversity', 'music_recency', 'music_consistency',
                'social_engagement', 'collaboration_score', 'discovery_score',
                'engagement_level', 'exploration_score'
            ]
            
            for feature in score_features:
                if feature in features and not (0 <= features[feature] <= 1):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating user features: {str(e)}")
            return False
    
    async def _process_music_traits(self, traits: Dict[str, Any]) -> Dict[str, Any]:
        """Process music-related traits."""
        features = {}
        
        # Process genres
        if genres := self._extract_genres(traits):
            features['genre_vector'] = self.category_embedding.embed_categories(genres)
        
        # Calculate artist diversity
        if artists := traits.get('artists', {}):
            features['artist_diversity'] = self._calculate_artist_diversity(artists)
            features['music_recency'] = self._calculate_music_recency(artists)
            features['music_consistency'] = self._calculate_music_consistency(artists)
        
        return features
    
    async def _process_social_traits(self, traits: Dict[str, Any]) -> Dict[str, Any]:
        """Process social-related traits."""
        return {
            'social_engagement': traits.get('social_score', 0.0),
            'collaboration_score': self._calculate_collaboration_score(traits),
            'discovery_score': traits.get('discovery_ratio', 0.0)
        }
    
    async def _process_professional_traits(self, traits: Dict[str, Any]) -> Dict[str, Any]:
        """Process professional-related traits."""
        features = {}
        
        # Combine LinkedIn and Google traits
        all_skills: Set[str] = set()
        all_interests: Set[str] = set()
        all_industries: Set[str] = set()
        
        for source in ['linkedin', 'google']:
            if source_traits := traits.get(source):
                if skills := source_traits.get('skills'):
                    all_skills.update(skills)
                if interests := source_traits.get('interests'):
                    all_interests.update(interests)
                if industries := source_traits.get('organizations'):
                    all_industries.update(industries)
        
        # Create embeddings
        features['professional_interests'] = list(all_interests)
        
        # Create skill vector by combining skill embeddings
        if all_skills:
            skill_embeddings = [
                self.text_embedding.embed_text(skill)
                for skill in all_skills
            ]
            features['skill_vector'] = np.mean(skill_embeddings, axis=0)
        
        # Create industry vector
        if all_industries:
            features['industry_vector'] = self.category_embedding.embed_categories(list(all_industries))
        
        return features
    
    async def _process_behavior_traits(self, traits: Dict[str, Any]) -> Dict[str, Any]:
        """Process behavior-related traits."""
        return {
            'activity_times': self._normalize_activity_times(traits.get('listening_times', {})),
            'engagement_level': traits.get('engagement_level', 0.0),
            'exploration_score': traits.get('discovery_ratio', 0.0)
        }
    
    def _extract_genres(self, traits: Dict[str, Any]) -> List[str]:
        """Extract unique genres from music traits."""
        genres = []
        if genre_data := traits.get('genres'):
            genres.extend(genre_data)
        return list(set(genres))
    
    def _calculate_artist_diversity(self, artists: Dict[str, List[Dict]]) -> float:
        """Calculate artist diversity score."""
        all_artists = []
        for term_data in artists.values():
            all_artists.extend(artist['name'] for artist in term_data)
        
        if not all_artists:
            return 0.0
            
        # Use entropy of artist distribution as diversity measure
        counts = Counter(all_artists)
        total = sum(counts.values())
        probabilities = [count/total for count in counts.values()]
        entropy = -sum(p * np.log(p) for p in probabilities)
        
        # Normalize to [0,1]
        max_entropy = np.log(len(counts))
        return entropy/max_entropy if max_entropy > 0 else 0.0
    
    def _calculate_music_recency(self, artists: Dict[str, List[Dict]]) -> float:
        """Calculate how much user prefers recent music."""
        short_term = len(artists.get('short_term', []))
        long_term = len(artists.get('long_term', []))
        
        if short_term + long_term == 0:
            return 0.5
            
        return short_term / (short_term + long_term)
    
    def _calculate_music_consistency(self, artists: Dict[str, List[Dict]]) -> float:
        """Calculate consistency in music taste."""
        short_term = set(a['name'] for a in artists.get('short_term', []))
        long_term = set(a['name'] for a in artists.get('long_term', []))
        
        if not short_term or not long_term:
            return 0.5
            
        # Jaccard similarity between short and long term
        intersection = len(short_term.intersection(long_term))
        union = len(short_term.union(long_term))
        
        return intersection / union if union > 0 else 0.0
    
    def _calculate_collaboration_score(self, traits: Dict[str, Any]) -> float:
        """Calculate collaboration tendency score."""
        total_playlists = traits.get('playlist_count', 0)
        if total_playlists == 0:
            return 0.0
            
        collab_playlists = traits.get('collaborative_playlists', 0)
        return collab_playlists / total_playlists
    
    def _normalize_activity_times(self, times: Dict[str, int]) -> Dict[str, float]:
        """Normalize activity times to probability distribution."""
        total = sum(times.values()) if times else 0
        if total == 0:
            return {str(i): 0.0 for i in range(24)}
            
        return {str(hour): count/total for hour, count in times.items()} 