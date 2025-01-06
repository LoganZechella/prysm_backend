from typing import List, Dict, Any, Optional
import logging
from collections import defaultdict
import spacy
from textblob import TextBlob
import re

logger = logging.getLogger(__name__)

class CategoryExtractor:
    def __init__(self):
        """Initialize the category extractor with NLP models and category mappings."""
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except Exception as e:
            logger.error(f"Error loading spaCy model: {str(e)}")
            self.nlp = None
            
        # Define main category hierarchies
        self.category_hierarchy = {
            "music": ["concert", "festival", "live music", "dj", "band", "orchestra"],
            "sports": ["game", "match", "tournament", "race", "competition"],
            "arts": ["exhibition", "gallery", "museum", "theater", "dance", "performance"],
            "food": ["dining", "tasting", "culinary", "restaurant", "food festival"],
            "education": ["workshop", "seminar", "conference", "lecture", "class"],
            "networking": ["meetup", "social", "networking", "mixer", "business"],
            "entertainment": ["show", "comedy", "movie", "film", "party"],
            "outdoor": ["hiking", "camping", "adventure", "nature", "outdoor"],
            "technology": ["tech", "hackathon", "coding", "digital", "software"],
            "wellness": ["fitness", "yoga", "meditation", "health", "wellness"]
        }
        
        # Create reverse mapping for faster lookups
        self.keyword_to_category = {}
        for category, keywords in self.category_hierarchy.items():
            for keyword in keywords:
                self.keyword_to_category[keyword] = category
                
    def extract_categories(self, event_data: Dict[str, Any]) -> List[str]:
        """
        Extract categories from event data using NLP and rule-based approaches.
        
        Args:
            event_data: Dictionary containing event information with fields:
                - title: string
                - description: string
                - tags: list of strings (optional)
                
        Returns:
            List of extracted categories
        """
        try:
            categories = set()
            
            # Process title and description
            text = f"{event_data.get('title', '')} {event_data.get('description', '')}"
            
            # Add any explicit tags
            tags = event_data.get('tags', [])
            if tags:
                text = f"{text} {' '.join(tags)}"
                
            # Clean and normalize text
            text = self._preprocess_text(text)
            
            # Extract categories using different methods
            categories.update(self._extract_from_keywords(text))
            categories.update(self._extract_from_nlp(text))
            
            # If no categories found, try to infer from context
            if not categories:
                categories.add(self._infer_default_category(text))
                
            return list(categories)
            
        except Exception as e:
            logger.error(f"Error extracting categories: {str(e)}")
            return ["uncategorized"]
            
    def _preprocess_text(self, text: str) -> str:
        """Clean and normalize text for processing."""
        if not text:
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text
        
    def _extract_from_keywords(self, text: str) -> set:
        """Extract categories based on keyword matching."""
        categories = set()
        
        for keyword, category in self.keyword_to_category.items():
            if keyword in text:
                categories.add(category)
                
        return categories
        
    def _extract_from_nlp(self, text: str) -> set:
        """Extract categories using NLP analysis."""
        categories = set()
        
        if not self.nlp:
            return categories
            
        try:
            # Process text with spaCy
            doc = self.nlp(text)
            
            # Extract entities and noun phrases
            entities = [ent.text.lower() for ent in doc.ents]
            noun_phrases = [chunk.text.lower() for chunk in doc.noun_chunks]
            
            # Check entities and noun phrases against keywords
            for item in entities + noun_phrases:
                for keyword, category in self.keyword_to_category.items():
                    if keyword in item:
                        categories.add(category)
                        
            # Use TextBlob for additional analysis
            blob = TextBlob(text)
            
            # Check noun phrases from TextBlob
            for phrase in str(blob.noun_phrases).split():
                for keyword, category in self.keyword_to_category.items():
                    if keyword in phrase:
                        categories.add(category)
                        
        except Exception as e:
            logger.error(f"Error in NLP processing: {str(e)}")
            
        return categories
        
    def _infer_default_category(self, text: str) -> str:
        """Infer a default category when no clear categories are found."""
        # Simple heuristic based on common words
        entertainment_words = {"fun", "event", "show", "entertainment"}
        education_words = {"learn", "study", "education", "training"}
        social_words = {"meet", "social", "community", "network"}
        
        words = set(text.split())
        
        if words & entertainment_words:
            return "entertainment"
        elif words & education_words:
            return "education"
        elif words & social_words:
            return "networking"
            
        return "uncategorized"
        
    def get_category_confidence(self, category: str, text: str) -> float:
        """
        Calculate confidence score for a category assignment.
        
        Args:
            category: Category to check
            text: Text to analyze
            
        Returns:
            Confidence score between 0 and 1
        """
        try:
            if not text or not category:
                return 0.0
                
            text = self._preprocess_text(text)
            
            # Get keywords for the category
            keywords = self.category_hierarchy.get(category, [])
            if not keywords:
                return 0.0
                
            # Count keyword matches
            matches = sum(1 for keyword in keywords if keyword in text)
            
            # Calculate base confidence from keyword matches
            base_confidence = min(1.0, matches / len(keywords))
            
            # Adjust confidence based on text analysis
            if self.nlp:
                doc = self.nlp(text)
                
                # Check for relevant entities
                entity_boost = 0.1 * sum(1 for ent in doc.ents if any(
                    keyword in ent.text.lower() for keyword in keywords
                ))
                
                # Use TextBlob for sentiment analysis
                blob = TextBlob(text)
                sentiment_tuple = blob.sentiment
                sentiment_score = float(sentiment_tuple[0])  # Access polarity directly from tuple
                sentiment_factor = 1.0 + (0.1 * abs(sentiment_score))  # Boost for strong sentiment
                
                final_confidence = min(1.0, base_confidence * sentiment_factor + entity_boost)
                return final_confidence
                
            return base_confidence
            
        except Exception as e:
            logger.error(f"Error calculating category confidence: {str(e)}")
            return 0.0 