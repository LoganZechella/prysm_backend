from typing import Dict, List, Set, Optional
from app.utils.schema import CategoryHierarchy
import spacy
from collections import Counter
import logging

logger = logging.getLogger(__name__)

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logger.warning("Downloading spaCy model...")
    spacy.cli.download("en_core_web_sm") # type: ignore
    nlp = spacy.load("en_core_web_sm")

def create_default_hierarchy() -> CategoryHierarchy:
    """Create the default category hierarchy"""
    hierarchy = CategoryHierarchy()
    
    # Add root categories
    hierarchy.add_category("events", keywords=["event", "happening", "occasion"])
    
    # Arts & Culture
    hierarchy.add_category(
        "arts_culture",
        parent="events",
        keywords=["art", "culture", "creative"],
        platform_mappings={
            "eventbrite": ["Arts", "Culture"],
            "meta": ["ARTS_ENTERTAINMENT", "CULTURAL"]
        }
    )
    
    hierarchy.add_category(
        "visual_arts",
        parent="arts_culture",
        keywords=["painting", "sculpture", "gallery", "exhibition", "museum"],
        platform_mappings={
            "eventbrite": ["Visual Arts", "Fine Art", "Exhibition"],
            "meta": ["ART_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "performing_arts",
        parent="arts_culture",
        keywords=["theater", "dance", "ballet", "opera", "performance"],
        platform_mappings={
            "eventbrite": ["Performing Arts", "Theatre", "Dance"],
            "meta": ["THEATER_EVENT", "DANCE_EVENT"]
        }
    )
    
    # Music
    hierarchy.add_category(
        "music",
        parent="events",
        keywords=["music", "concert", "gig", "festival"],
        platform_mappings={
            "eventbrite": ["Music", "Concerts"],
            "meta": ["MUSIC_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "live_music",
        parent="music",
        keywords=["live", "band", "performance", "stage"],
        platform_mappings={
            "eventbrite": ["Live Music", "Band"],
            "meta": ["MUSIC_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "electronic_music",
        parent="music",
        keywords=["electronic", "dj", "edm", "techno", "house"],
        platform_mappings={
            "eventbrite": ["EDM / Electronic", "DJ"],
            "meta": ["MUSIC_EVENT"]
        }
    )
    
    # Food & Drink
    hierarchy.add_category(
        "food_drink",
        parent="events",
        keywords=["food", "drink", "culinary", "dining"],
        platform_mappings={
            "eventbrite": ["Food & Drink"],
            "meta": ["FOOD_EVENT", "DRINK_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "food_tasting",
        parent="food_drink",
        keywords=["tasting", "sampling", "flavors", "cuisine"],
        platform_mappings={
            "eventbrite": ["Food Tasting", "Wine Tasting"],
            "meta": ["FOOD_TASTING"]
        }
    )
    
    # Sports & Fitness
    hierarchy.add_category(
        "sports_fitness",
        parent="events",
        keywords=["sports", "fitness", "athletic", "exercise"],
        platform_mappings={
            "eventbrite": ["Sports & Fitness"],
            "meta": ["FITNESS_EVENT", "SPORTS_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "outdoor_sports",
        parent="sports_fitness",
        keywords=["outdoor", "hiking", "climbing", "cycling"],
        platform_mappings={
            "eventbrite": ["Outdoor Sports"],
            "meta": ["OUTDOOR_EVENT"]
        }
    )
    
    # Business & Professional
    hierarchy.add_category(
        "business",
        parent="events",
        keywords=["business", "professional", "corporate", "industry"],
        platform_mappings={
            "eventbrite": ["Business", "Professional"],
            "meta": ["BUSINESS_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "networking",
        parent="business",
        keywords=["networking", "mixer", "meetup", "social"],
        platform_mappings={
            "eventbrite": ["Networking"],
            "meta": ["NETWORKING_EVENT"]
        }
    )
    
    return hierarchy

def extract_categories(text: str, hierarchy: CategoryHierarchy) -> Set[str]:
    """
    Extract categories from text using NLP and hierarchical matching.
    Returns a set of category names.
    """
    # Process text with spaCy
    doc = nlp(text.lower())
    
    # Extract relevant phrases and entities
    phrases = set()
    
    # Add noun phrases
    phrases.update(chunk.text.lower() for chunk in doc.noun_chunks)
    
    # Add named entities
    phrases.update(ent.text.lower() for ent in doc.ents)
    
    # Add individual tokens (excluding stop words and punctuation)
    phrases.update(
        token.text.lower() for token in doc 
        if not token.is_stop and not token.is_punct
    )
    
    # Score each category based on keyword matches
    category_scores: Dict[str, int] = Counter()
    
    for category, node in hierarchy.nodes.items():
        score = 0
        # Check each phrase against category keywords
        for phrase in phrases:
            if any(keyword in phrase for keyword in node.keywords):
                score += 1
            # Exact match gets higher score
            if phrase in node.keywords:
                score += 2
        
        if score > 0:
            category_scores[category] = score
    
    # Get categories with scores above threshold
    threshold = 1
    matched_categories = {
        category for category, score in category_scores.items()
        if score >= threshold
    }
    
    # Add parent categories where appropriate
    all_categories = set()
    for category in matched_categories:
        all_categories.add(category)
        all_categories.update(hierarchy.get_ancestors(category))
    
    return all_categories

def map_platform_categories(platform: str, platform_categories: List[str], 
                          hierarchy: CategoryHierarchy) -> Set[str]:
    """Map platform-specific categories to our hierarchy"""
    mapped_categories = set()
    
    for platform_category in platform_categories:
        mapped = hierarchy.map_platform_category(platform, platform_category)
        if mapped:
            mapped_categories.add(mapped)
            # Add ancestors
            mapped_categories.update(hierarchy.get_ancestors(mapped))
    
    return mapped_categories 