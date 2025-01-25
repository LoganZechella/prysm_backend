"""Content-based filtering implementation."""
import logging
from typing import Dict, Any, List, Tuple
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)

class ContentBasedFilter:
    """Filter for scoring events based on content similarity to user preferences."""
    
    def __init__(self):
        """Initialize the content-based filter."""
        self.vectorizer = TfidfVectorizer(
            analyzer='word',
            stop_words='english',
            max_features=5000,
            ngram_range=(1, 2)
        )
        
    def calculate_similarity(
        self,
        user_vector: Dict[str, Any],
        event: Dict[str, Any]
    ) -> float:
        """
        Calculate similarity between user preferences and event.
        
        Args:
            user_vector: Dictionary containing user trait vectors
            event: Dictionary containing event information and features
            
        Returns:
            Similarity score between 0 and 1
        """
        try:
            scores = []
            weights = {
                "text_similarity": 0.4,
                "category_match": 0.2,
                "location_match": 0.2,
                "interest_match": 0.2
            }
            
            # Calculate text similarity
            text_score = self._calculate_text_similarity(user_vector, event)
            scores.append(("text_similarity", text_score))
            
            # Calculate category match
            category_score = self._calculate_category_match(user_vector, event)
            scores.append(("category_match", category_score))
            
            # Calculate location match
            location_score = self._calculate_location_match(user_vector, event)
            scores.append(("location_match", location_score))
            
            # Calculate interest match
            interest_score = self._calculate_interest_match(user_vector, event)
            scores.append(("interest_match", interest_score))
            
            # Calculate weighted average
            final_score = sum(
                weights[name] * score 
                for name, score in scores
            )
            
            logger.debug(f"Similarity scores for event {event.get('id')}: {dict(scores)}")
            logger.debug(f"Final score: {final_score}")
            
            return final_score
            
        except Exception as e:
            logger.error(f"Error calculating similarity: {str(e)}")
            return 0.0
            
    def _calculate_text_similarity(
        self,
        user_vector: Dict[str, Any],
        event: Dict[str, Any]
    ) -> float:
        """Calculate similarity between user interests and event text content."""
        try:
            # Combine user interests into a single text
            user_text = " ".join([
                *user_vector.get("interests", []),
                *[skill for skill in user_vector.get("professional", {}).get("skills", [])],
                *[industry for industry in user_vector.get("professional", {}).get("industries", [])]
            ])
            
            # Combine event text content
            event_text = " ".join(filter(None, [
                event.get("title", ""),
                event.get("description", ""),
                event.get("category", ""),
                event.get("subcategory", "")
            ]))
            
            if not user_text or not event_text:
                return 0.0
                
            # Vectorize texts
            texts = [user_text, event_text]
            tfidf_matrix = self.vectorizer.fit_transform(texts)
            
            # Calculate cosine similarity
            similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error calculating text similarity: {str(e)}")
            return 0.0
            
    def _calculate_category_match(
        self,
        user_vector: Dict[str, Any],
        event: Dict[str, Any]
    ) -> float:
        """Calculate match between user interests and event category."""
        try:
            event_category = event.get("category", "").lower()
            if not event_category:
                return 0.0
                
            # Check direct matches in interests
            interests = [
                interest.lower() 
                for interest in user_vector.get("interests", [])
            ]
            if event_category in interests:
                return 1.0
                
            # Check partial matches
            for interest in interests:
                if (
                    interest in event_category or 
                    event_category in interest
                ):
                    return 0.7
                    
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating category match: {str(e)}")
            return 0.0
            
    def _calculate_location_match(
        self,
        user_vector: Dict[str, Any],
        event: Dict[str, Any]
    ) -> float:
        """Calculate match between user location and event location."""
        try:
            user_location = user_vector.get("location")
            event_location = event.get("location")
            
            if not user_location or not event_location:
                return 0.0
                
            # For now, simple exact match
            # TODO: Implement proper location distance calculation
            if user_location.lower() == event_location.lower():
                return 1.0
                
            return 0.0
            
        except Exception as e:
            logger.error(f"Error calculating location match: {str(e)}")
            return 0.0
            
    def _calculate_interest_match(
        self,
        user_vector: Dict[str, Any],
        event: Dict[str, Any]
    ) -> float:
        """Calculate match between user interests and event metadata."""
        try:
            event_tags = set(
                tag.lower() 
                for tag in event.get("metadata", {}).get("tags", [])
            )
            if not event_tags:
                return 0.0
                
            # Combine all user interests
            user_interests = set(
                interest.lower() 
                for interest in (
                    user_vector.get("interests", []) +
                    user_vector.get("professional", {}).get("skills", []) +
                    user_vector.get("professional", {}).get("interests", [])
                )
            )
            
            if not user_interests:
                return 0.0
                
            # Calculate Jaccard similarity
            intersection = len(event_tags.intersection(user_interests))
            union = len(event_tags.union(user_interests))
            
            if union == 0:
                return 0.0
                
            return intersection / union
            
        except Exception as e:
            logger.error(f"Error calculating interest match: {str(e)}")
            return 0.0 