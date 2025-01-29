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
            
        # Define main category hierarchies with subcategories
        self.category_hierarchy = {
            "music": {
                "keywords": ["music", "concert", "festival", "live music", "dj", "band", "orchestra"],
                "subcategories": {
                    "jazz": ["jazz", "blues", "swing", "soul", "r&b"],
                    "rock": ["rock", "metal", "punk", "alternative"],
                    "classical": ["classical", "orchestra", "symphony", "chamber"],
                    "electronic": ["electronic", "edm", "techno", "house", "dubstep"]
                }
            },
            "sports": {
                "keywords": ["sports", "game", "match", "tournament", "race", "competition"],
                "subcategories": {
                    "team_sports": ["football", "basketball", "baseball", "soccer", "hockey"],
                    "individual_sports": ["tennis", "golf", "athletics", "swimming"]
                }
            },
            "arts": {
                "keywords": ["art", "exhibition", "gallery", "museum", "theater", "dance", "performance"],
                "subcategories": {
                    "visual_arts": ["painting", "sculpture", "photography", "drawing", "digital art"],
                    "performing_arts": ["theater", "dance", "ballet", "opera", "drama"]
                }
            },
            "food": {
                "keywords": ["dining", "tasting", "culinary", "restaurant", "food festival", "cuisine"],
                "subcategories": {}
            },
            "education": {
                "keywords": ["workshop", "seminar", "conference", "lecture", "class", "training"],
                "subcategories": {}
            },
            "networking": {
                "keywords": ["meetup", "social", "networking", "mixer", "business", "professional"],
                "subcategories": {}
            },
            "entertainment": {
                "keywords": ["show", "comedy", "movie", "film", "party", "fun"],
                "subcategories": {}
            },
            "outdoor": {
                "keywords": ["hiking", "camping", "adventure", "nature", "outdoor", "wilderness"],
                "subcategories": {}
            },
            "technology": {
                "keywords": ["tech", "hackathon", "coding", "digital", "software", "programming"],
                "subcategories": {}
            },
            "wellness": {
                "keywords": ["fitness", "yoga", "meditation", "health", "wellness", "mindfulness"],
                "subcategories": {}
            }
        }
        
        # Create reverse mapping for faster lookups
        self.keyword_to_category = {}
        for category, data in self.category_hierarchy.items():
            # Add main category keywords
            for keyword in data["keywords"]:
                self.keyword_to_category[keyword.lower()] = category
            # Add subcategory keywords
            for subcategory, keywords in data["subcategories"].items():
                for keyword in keywords:
                    self.keyword_to_category[keyword.lower()] = subcategory
                
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
            
            if not text.strip():
                return []
                
            # Extract categories using different methods
            categories.update(self._extract_from_keywords(text))
            categories.update(self._extract_from_nlp(text))
            
            # If no categories found, try to infer from context
            if not categories:
                inferred = self._infer_default_category(text)
                if inferred:  # Only add if not empty string
                    categories.add(inferred)
                
            return list(categories) if categories else []
            
        except Exception as e:
            logger.error(f"Error extracting categories: {str(e)}")
            return []
            
    def _preprocess_text(self, text: str) -> str:
        """Clean and normalize text for processing."""
        if not text:
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters but keep spaces between words
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text
        
    def _extract_from_keywords(self, text: str) -> set:
        """Extract categories based on keyword matching."""
        categories = set()
        words = text.split()
        
        # Check single words
        for word in words:
            word = word.lower()
            if word in self.keyword_to_category:
                cat = self.keyword_to_category[word]
                categories.add(cat)
                # Add parent category if this is a subcategory
                for main_cat, data in self.category_hierarchy.items():
                    if cat in data["subcategories"]:
                        categories.add(main_cat)
                        # Add the keyword itself as a category if it's a subcategory keyword
                        for subcat, keywords in data["subcategories"].items():
                            if word in [k.lower() for k in keywords]:
                                categories.add(word)
        
        # Check multi-word phrases
        for category, data in self.category_hierarchy.items():
            # Check main category keywords
            for keyword in data["keywords"]:
                if keyword.lower() in text.lower():
                    categories.add(category)
            # Check subcategory keywords
            for subcategory, keywords in data["subcategories"].items():
                for keyword in keywords:
                    if keyword.lower() in text.lower():
                        categories.add(subcategory)
                        categories.add(category)  # Add parent category too
                        categories.add(keyword.lower())  # Add the keyword itself as a category
                
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
                # Check single words
                for word in item.split():
                    word = word.lower()
                    if word in self.keyword_to_category:
                        cat = self.keyword_to_category[word]
                        categories.add(cat)
                        # Add parent category if this is a subcategory
                        for main_cat, data in self.category_hierarchy.items():
                            if cat in data["subcategories"]:
                                categories.add(main_cat)
                                # Add the keyword itself as a category if it's a subcategory keyword
                                for subcat, keywords in data["subcategories"].items():
                                    if word in [k.lower() for k in keywords]:
                                        categories.add(word)
                
                # Check multi-word phrases
                for category, data in self.category_hierarchy.items():
                    # Check main category keywords
                    if any(keyword.lower() in item.lower() for keyword in data["keywords"]):
                        categories.add(category)
                    # Check subcategory keywords
                    for subcategory, keywords in data["subcategories"].items():
                        for keyword in keywords:
                            if keyword.lower() in item.lower():
                                categories.add(subcategory)
                                categories.add(category)  # Add parent category too
                                categories.add(keyword.lower())  # Add the keyword itself as a category
                        
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
            
        return ""  # Return empty string instead of "uncategorized"

# Module-level extractor instance
_category_extractor = CategoryExtractor()

def extract_categories_from_text(text: str) -> List[str]:
    """
    Extract categories from text using NLP and rule-based approaches.
    
    Args:
        text: Text to analyze for categories
        
    Returns:
        List of extracted categories
    """
    return _category_extractor.extract_categories({"description": text}) 