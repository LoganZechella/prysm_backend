import logging
from typing import List, Dict, Set, Optional
from dataclasses import dataclass
import spacy
from collections import defaultdict
from app.utils.schema import Event

logger = logging.getLogger(__name__)

@dataclass
class CategoryNode:
    """Represents a node in the category hierarchy."""
    name: str
    parent: Optional[str]
    children: Set[str]
    keywords: Set[str]
    level: int  # 0 = root, 1 = main category, 2 = subcategory, etc.

# Define category hierarchy
CATEGORY_HIERARCHY = {
    "music": CategoryNode(
        name="music",
        parent=None,
        children={"concert", "festival", "live-music", "classical", "jazz", "rock"},
        keywords={"band", "concert", "performance", "stage", "tour", "musician", "orchestra"},
        level=1
    ),
    "concert": CategoryNode(
        name="concert",
        parent="music",
        children=set(),
        keywords={"live", "performance", "stage", "venue", "tour"},
        level=2
    ),
    "festival": CategoryNode(
        name="festival",
        parent="music",
        children=set(),
        keywords={"fest", "festival", "celebration", "outdoor", "lineup"},
        level=2
    ),
    "arts": CategoryNode(
        name="arts",
        parent=None,
        children={"exhibition", "gallery", "theater", "dance", "performance-art"},
        keywords={"art", "artist", "creative", "exhibition", "gallery", "museum"},
        level=1
    ),
    "exhibition": CategoryNode(
        name="exhibition",
        parent="arts",
        children=set(),
        keywords={"exhibit", "gallery", "showcase", "display", "collection"},
        level=2
    ),
    "sports": CategoryNode(
        name="sports",
        parent=None,
        children={"game", "match", "tournament", "competition", "race"},
        keywords={"sports", "game", "match", "tournament", "championship", "competition"},
        level=1
    ),
    "food-drink": CategoryNode(
        name="food-drink",
        parent=None,
        children={"tasting", "dinner", "cooking", "wine", "beer"},
        keywords={"food", "drink", "tasting", "culinary", "dining", "restaurant", "chef"},
        level=1
    )
}

def extract_categories_from_text(text: str, nlp=None) -> Set[str]:
    """
    Extract potential categories from text using NLP.
    Returns set of category keywords found.
    """
    if not text:
        return set()
    
    # Load spaCy model if not provided
    if nlp is None:
        nlp = spacy.load("en_core_web_sm")
    
    # Process text
    doc = nlp(text.lower())
    
    # Extract nouns and noun phrases
    keywords = set()
    for token in doc:
        if token.pos_ in {"NOUN", "PROPN"}:
            keywords.add(token.text)
    
    for chunk in doc.noun_chunks:
        keywords.add(chunk.root.text)
    
    return keywords

def match_categories(keywords: Set[str], min_confidence: float = 0.3) -> Dict[str, float]:
    """
    Match extracted keywords to category hierarchy.
    Returns dict of category names and confidence scores.
    """
    category_scores = defaultdict(float)
    
    for category, node in CATEGORY_HIERARCHY.items():
        # Calculate overlap between keywords and category keywords
        overlap = len(keywords.intersection(node.keywords))
        if overlap > 0:
            # Base confidence score on overlap and keyword relevance
            confidence = min(1.0, overlap / len(node.keywords))
            category_scores[category] = confidence
            
            # Add parent category with reduced confidence
            if node.parent:
                parent_confidence = confidence * 0.8  # Reduce confidence for parent
                category_scores[node.parent] = max(
                    category_scores[node.parent],
                    parent_confidence
                )
    
    # Filter out low confidence categories
    return {
        category: score
        for category, score in category_scores.items()
        if score >= min_confidence
    }

def extract_hierarchical_categories(event: Event, nlp=None) -> Dict[str, float]:
    """
    Extract hierarchical categories from an event.
    Returns dict of category names and confidence scores.
    """
    # Combine text fields for analysis
    text_fields = [
        event.title,
        event.description,
        event.short_description,
        " ".join(event.categories),  # Include existing categories
        event.location.venue_name
    ]
    combined_text = " ".join(filter(None, text_fields))
    
    # Extract keywords from text
    keywords = extract_categories_from_text(combined_text, nlp)
    
    # Match keywords to categories
    categories = match_categories(keywords)
    
    # Boost confidence for existing categories
    for category in event.categories:
        if category in categories:
            categories[category] = min(1.0, categories[category] * 1.2)
    
    return categories

def get_category_hierarchy(category: str) -> List[str]:
    """
    Get full hierarchy path for a category.
    Returns list from root to leaf category.
    """
    if category not in CATEGORY_HIERARCHY:
        return []
    
    hierarchy = [category]
    current = CATEGORY_HIERARCHY[category]
    
    while current.parent:
        hierarchy.insert(0, current.parent)
        current = CATEGORY_HIERARCHY[current.parent]
    
    return hierarchy

def enrich_event_categories(event: Event, nlp=None) -> Event:
    """
    Enrich event with hierarchical categories.
    Updates event.categories with most confident categories.
    Returns updated event.
    """
    # Extract categories with confidence scores
    categories = extract_hierarchical_categories(event, nlp)
    
    if not categories:
        return event
    
    # Sort by confidence
    sorted_categories = sorted(
        categories.items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    # Take top categories and their parents
    enriched_categories = set()
    for category, _ in sorted_categories[:3]:  # Top 3 categories
        hierarchy = get_category_hierarchy(category)
        enriched_categories.update(hierarchy)
    
    # Update event categories
    event.categories = list(enriched_categories)
    
    return event 