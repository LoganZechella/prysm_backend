"""Processor for extracting features from events."""
from typing import Dict, Any, List
from datetime import datetime
import numpy as np
from collections import defaultdict
import logging

from .base import BaseFeatureProcessor
from app.services.nlp_service import NLPService
from app.recommendation.models.embeddings import TextEmbedding, CategoryEmbedding
from app.recommendation.models.topic_model import TopicModel

logger = logging.getLogger(__name__)

class EventFeatureProcessor(BaseFeatureProcessor):
    """Processor for event features."""
    
    def __init__(
        self,
        nlp_service: NLPService,
        text_embedding: TextEmbedding,
        category_embedding: CategoryEmbedding,
        topic_model: TopicModel
    ):
        """Initialize the event feature processor.
        
        Args:
            nlp_service: NLP service for text processing
            text_embedding: Text embedding model
            category_embedding: Category embedding model
            topic_model: Topic modeling component
        """
        super().__init__()
        self.nlp_service = nlp_service
        self.text_embedding = text_embedding
        self.category_embedding = category_embedding
        self.topic_model = topic_model
        
        # Define feature categories
        self._feature_names = [
            # Content features
            "title_embedding",
            "description_embedding",
            "category_vector",
            "topic_vector",
            
            # Metadata features
            "price_normalized",
            "capacity_normalized",
            "time_of_day",
            "day_of_week",
            "duration_hours",
            
            # Social features
            "attendance_score",
            "social_score",
            "organizer_rating"
        ]
        
    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process event data into feature vectors.
        
        Args:
            data: Raw event data
            
        Returns:
            Dictionary containing processed features
        """
        try:
            with self.performance_monitor.monitor_operation('process_event_features'):
                features = {}
                
                # Process text content
                if title := data.get('title'):
                    # Get NLP analysis
                    title_analysis = await self.nlp_service.analyze_text(title)
                    
                    # Create text embedding
                    features['title_embedding'] = self.text_embedding.embed_text(title)
                
                if description := data.get('description'):
                    # Get NLP analysis
                    desc_analysis = await self.nlp_service.analyze_text(description)
                    
                    # Create text embedding
                    features['description_embedding'] = self.text_embedding.embed_text(description)
                    
                    # Extract topics
                    features['topic_vector'], _ = self.topic_model.extract_topics(description)
                
                # Process categories
                if categories := data.get('categories', []):
                    features['category_vector'] = self.category_embedding.embed_categories(categories)
                
                # Process metadata
                features.update(self._process_metadata(data))
                
                # Process social signals
                features.update(self._process_social_signals(data))
                
                self.last_processed = datetime.utcnow()
                return features
                
        except Exception as e:
            logger.error(f"Error processing event features: {str(e)}")
            raise
    
    async def validate(self, features: Dict[str, Any]) -> bool:
        """Validate processed event features.
        
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
            vector_features = [
                'title_embedding', 'description_embedding',
                'category_vector', 'topic_vector'
            ]
            
            for key in vector_features:
                if key in features and not isinstance(features[key], np.ndarray):
                    return False
            
            # Validate normalized values
            normalized_features = [
                'price_normalized', 'capacity_normalized',
                'attendance_score', 'social_score', 'organizer_rating'
            ]
            
            for feature in normalized_features:
                if feature in features and not (0 <= features[feature] <= 1):
                    return False
            
            # Validate time features
            if 'time_of_day' in features and not (0 <= features['time_of_day'] < 24):
                return False
                
            if 'day_of_week' in features and not (0 <= features['day_of_week'] < 7):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating event features: {str(e)}")
            return False
    
    def _process_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process event metadata features."""
        features = {}
        
        # Process price
        if price := data.get('price'):
            features['price_normalized'] = self._normalize_price(price)
        
        # Process capacity
        if capacity := data.get('capacity'):
            features['capacity_normalized'] = self._normalize_capacity(capacity)
        
        # Process time
        if start_time := data.get('start_time'):
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            features['time_of_day'] = start_time.hour
            features['day_of_week'] = start_time.weekday()
        
        # Process duration
        if (start_time := data.get('start_time')) and (end_time := data.get('end_time')):
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            if isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            duration = (end_time - start_time).total_seconds() / 3600  # Convert to hours
            features['duration_hours'] = max(0, min(duration, 24))  # Cap at 24 hours
        
        return features
    
    def _process_social_signals(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process social signal features."""
        features = {}
        
        # Calculate attendance score
        if (capacity := data.get('capacity', 0)) and (attending := data.get('attending_count', 0)):
            features['attendance_score'] = min(1.0, attending / capacity) if capacity > 0 else 0.0
        
        # Calculate social score
        interested = data.get('interested_count', 0)
        shares = data.get('shares_count', 0)
        comments = data.get('comments_count', 0)
        
        if any([interested, shares, comments]):
            # Weighted combination of social signals
            features['social_score'] = self._calculate_social_score(interested, shares, comments)
        
        # Process organizer rating
        if rating := data.get('organizer_rating'):
            features['organizer_rating'] = rating / 5.0  # Assuming 5-star scale
        
        return features
    
    def _normalize_price(self, price: float) -> float:
        """Normalize price to [0,1] range."""
        if price <= 0:
            return 0.0
        return min(1.0, price / 1000)  # Cap at $1000 for now
    
    def _normalize_capacity(self, capacity: int) -> float:
        """Normalize capacity to [0,1] range."""
        if capacity <= 0:
            return 0.0
        return min(1.0, capacity / 1000)  # Cap at 1000 for now
    
    def _calculate_social_score(self, interested: int, shares: int, comments: int) -> float:
        """Calculate normalized social engagement score."""
        # Weighted combination of different signals
        weights = {
            'interested': 1.0,
            'shares': 2.0,
            'comments': 1.5
        }
        
        score = (
            weights['interested'] * interested +
            weights['shares'] * shares +
            weights['comments'] * comments
        )
        
        # Normalize using a soft maximum
        return 1 - (1 / (1 + score/100)) 