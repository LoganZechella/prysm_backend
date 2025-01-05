from typing import Set, Dict, List
import spacy
from app.schemas.event import Event
from app.schemas.category import CategoryHierarchy

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

def extract_categories_from_text(text: str) -> Set[str]:
    """
    Extract potential categories from text using NLP.
    
    Args:
        text: Text to analyze
        
    Returns:
        Set of extracted categories
    """
    if not text:
        return set()
        
    # Process text with spaCy
    doc = nlp(text.lower())
    
    # Extract nouns and noun phrases
    categories = set()
    for token in doc:
        if token.pos_ in {"NOUN", "PROPN"}:
            categories.add(token.lemma_)
            
    for chunk in doc.noun_chunks:
        categories.add(chunk.root.lemma_)
        
    # Filter categories against known category list
    valid_categories = {"music", "concert", "festival", "sports", "arts", 
                       "theater", "dance", "comedy", "food", "drink",
                       "jazz", "blues", "rock", "classical", "pop"}
                       
    return categories.intersection(valid_categories)

def get_category_hierarchy(category: str) -> List[str]:
    """
    Get the hierarchical path for a category.
    
    Args:
        category: Category to get hierarchy for
        
    Returns:
        List of categories from root to leaf
    """
    # Define category hierarchy
    hierarchy = CategoryHierarchy()
    
    # Add root categories
    hierarchy.add_category("events")
    hierarchy.add_category("music", parent="events")
    hierarchy.add_category("sports", parent="events")
    hierarchy.add_category("arts", parent="events")
    
    # Add music subcategories
    hierarchy.add_category("concert", parent="music")
    hierarchy.add_category("festival", parent="music")
    hierarchy.add_category("jazz", parent="music")
    hierarchy.add_category("blues", parent="music")
    hierarchy.add_category("rock", parent="music")
    hierarchy.add_category("classical", parent="music")
    hierarchy.add_category("pop", parent="music")
    
    # Add arts subcategories
    hierarchy.add_category("theater", parent="arts")
    hierarchy.add_category("dance", parent="arts")
    hierarchy.add_category("comedy", parent="arts")
    
    # Get ancestors for category
    if category in hierarchy.nodes:
        return hierarchy.get_ancestors(category) + [category]
    return []

def extract_hierarchical_categories(event: Event) -> Dict[str, float]:
    """
    Extract categories with confidence scores from event data.
    
    Args:
        event: Event to extract categories from
        
    Returns:
        Dictionary mapping categories to confidence scores
    """
    # Extract categories from title and description
    text = f"{event.title} {event.description}"
    categories = extract_categories_from_text(text)
    
    # Calculate confidence scores and add hierarchical categories
    scores = {}
    for category in categories:
        # Direct matches get high confidence
        scores[category] = 0.8
        
        # Add parent categories with lower confidence
        for parent in get_category_hierarchy(category)[:-1]:  # Exclude the category itself
            if parent not in scores:
                scores[parent] = 0.5
            else:
                scores[parent] = max(scores[parent], 0.5)
                
    return scores

def enrich_event_categories(event: Event) -> Event:
    """
    Enrich event with extracted and hierarchical categories.
    
    Args:
        event: Event to enrich
        
    Returns:
        Enriched event with updated categories
    """
    # Extract categories with confidence scores
    category_scores = extract_hierarchical_categories(event)
    
    # Update event categories, keeping existing ones
    event.categories = list(set(event.categories).union(category_scores.keys()))
    
    return event 