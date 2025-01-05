from typing import Dict, List, Any, Set
from datetime import datetime
from app.schemas.event import Event
from app.services.location_recommendations import calculate_distance
import spacy

# Load spaCy model for topic extraction
nlp = spacy.load("en_core_web_sm")

def clean_event_data(event: Event) -> Event:
    """
    Clean and normalize event data.
    
    Args:
        event: Event to clean
        
    Returns:
        Cleaned event
    """
    # Clean text fields
    event.title = event.title.strip()
    event.description = " ".join(event.description.split())
    
    # Clean location fields
    if event.location:
        if event.location.state:
            event.location.state = event.location.state.strip()
        if event.location.country:
            event.location.country = event.location.country.strip()
            
    return event

def extract_topics(text: str) -> Set[str]:
    """
    Extract topics from text using NLP.
    
    Args:
        text: Text to analyze
        
    Returns:
        Set of extracted topics
    """
    # Process text with spaCy
    doc = nlp(text.lower())
    
    # Extract nouns and noun phrases
    topics = set()
    for token in doc:
        if token.pos_ in {"NOUN", "PROPN"}:
            topics.add(token.lemma_)
            
    for chunk in doc.noun_chunks:
        topics.add(chunk.root.lemma_)
        
    # Filter topics against known list
    valid_topics = {"music", "jazz", "blues", "rock", "classical", "sports",
                   "arts", "theater", "dance", "comedy", "food", "drink"}
                   
    return topics.intersection(valid_topics)

def calculate_event_scores(event: Event, user_preferences: Dict[str, Any]) -> Dict[str, float]:
    """
    Calculate various scores for an event based on user preferences.
    
    Args:
        event: Event to score
        user_preferences: User preferences
        
    Returns:
        Dictionary of scores
    """
    scores = {}
    
    # Calculate category score
    preferred_categories = set(user_preferences.get("preferred_categories", []))
    excluded_categories = set(user_preferences.get("excluded_categories", []))
    event_categories = set(event.categories)
    
    if excluded_categories & event_categories:
        scores["category_score"] = 0.0
    else:
        matching_categories = preferred_categories & event_categories
        scores["category_score"] = len(matching_categories) / max(len(preferred_categories), 1)
        
    # Calculate price score
    if event.price_info:
        min_price = user_preferences.get("min_price", 0)
        max_price = user_preferences.get("max_price", float("inf"))
        event_price = event.price_info.max_price
        
        if min_price <= event_price <= max_price:
            scores["price_score"] = 1.0
        else:
            price_range = max_price - min_price
            if price_range > 0:
                distance_from_range = min(abs(event_price - min_price),
                                       abs(event_price - max_price))
                scores["price_score"] = max(0, 1 - (distance_from_range / price_range))
            else:
                scores["price_score"] = 0.0
    else:
        scores["price_score"] = 1.0  # Free events get maximum score
        
    # Calculate distance score
    if event.location and event.location.coordinates and user_preferences.get("preferred_location"):
        distance = calculate_distance(
            event.location.coordinates,
            user_preferences["preferred_location"]
        )
        max_distance = user_preferences.get("max_distance", 50)  # Default 50km
        scores["distance_score"] = max(0, 1 - (distance / max_distance))
    else:
        scores["distance_score"] = 0.5  # Neutral score if location info missing
        
    # Add popularity score
    scores["popularity_score"] = event.metadata.popularity_score if event.metadata else 0.5
    
    # Calculate total score
    weights = {
        "category_score": 0.4,
        "price_score": 0.2,
        "distance_score": 0.3,
        "popularity_score": 0.1
    }
    
    scores["total_score"] = sum(score * weights[name] for name, score in scores.items()
                               if name != "total_score")
                               
    return scores 