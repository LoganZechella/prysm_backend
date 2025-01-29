from typing import Dict, List, Set, Optional, Any
from app.schemas.category import CategoryHierarchy, CategoryNode
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
        "jazz",
        parent="music",
        keywords=["jazz", "blues", "swing"],
        platform_mappings={
            "eventbrite": ["Jazz", "Blues"],
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
    
    # Sports
    hierarchy.add_category(
        "sports",
        parent="events",
        keywords=["sports", "game", "match", "tournament"],
        platform_mappings={
            "eventbrite": ["Sports"],
            "meta": ["SPORTS_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "team_sports",
        parent="sports",
        keywords=["football", "basketball", "baseball", "soccer"],
        platform_mappings={
            "eventbrite": ["Team Sports"],
            "meta": ["SPORTS_EVENT"]
        }
    )
    
    hierarchy.add_category(
        "individual_sports",
        parent="sports",
        keywords=["tennis", "golf", "athletics"],
        platform_mappings={
            "eventbrite": ["Individual Sports"],
            "meta": ["SPORTS_EVENT"]
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

def map_platform_categories(platform_categories: List[str], platform: str, 
                          hierarchy: CategoryHierarchy) -> Set[str]:
    """Map platform-specific categories to our hierarchy"""
    mapped_categories = set()
    
    for platform_category in platform_categories:
        # Try direct mapping first
        for category, node in hierarchy.nodes.items():
            if node.platform_mappings and platform in node.platform_mappings:
                if platform_category in node.platform_mappings[platform]:
                    mapped_categories.add(category)
                    # Add ancestors
                    mapped_categories.update(hierarchy.get_ancestors(category))
                    break
        
        # If no direct mapping found, try keyword matching
        if not mapped_categories:
            text = platform_category.lower()
            for category, node in hierarchy.nodes.items():
                if any(keyword in text for keyword in node.keywords):
                    mapped_categories.add(category)
                    # Add ancestors
                    mapped_categories.update(hierarchy.get_ancestors(category))
    
    return mapped_categories

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

class CategoryNode:
    """Node in the category hierarchy tree."""
    
    def __init__(self, name: str):
        """Initialize category node."""
        self.name = name
        self.children = []
        self.parent = None
        
    def add_child(self, child: 'CategoryNode') -> None:
        """Add a child node."""
        self.children.append(child)
        child.parent = self
        
    def get_hierarchy(self) -> Dict[str, Any]:
        """Get hierarchy as dictionary."""
        return {
            'name': self.name,
            'children': [child.get_hierarchy() for child in self.children]
        }
        
    def get_ancestors(self) -> List[str]:
        """Get list of ancestor category names."""
        ancestors = []
        current = self.parent
        while current:
            ancestors.append(current.name)
            current = current.parent
        return ancestors
        
    def get_descendants(self) -> List[str]:
        """Get list of descendant category names."""
        descendants = []
        for child in self.children:
            descendants.append(child.name)
            descendants.extend(child.get_descendants())
        return descendants

def get_category_hierarchy() -> Dict[str, Any]:
    """
    Get the full category hierarchy.
    
    Returns:
        Dictionary representing the category tree
    """
    # Create root categories
    root = CategoryNode("root")
    
    # Arts & Entertainment
    arts = CategoryNode("arts")
    arts.add_child(CategoryNode("music"))
    arts.add_child(CategoryNode("theater"))
    arts.add_child(CategoryNode("dance"))
    arts.add_child(CategoryNode("comedy"))
    arts.add_child(CategoryNode("film"))
    root.add_child(arts)
    
    # Sports & Fitness
    sports = CategoryNode("sports")
    sports.add_child(CategoryNode("running"))
    sports.add_child(CategoryNode("yoga"))
    sports.add_child(CategoryNode("cycling"))
    sports.add_child(CategoryNode("martial-arts"))
    root.add_child(sports)
    
    # Food & Drink
    food = CategoryNode("food")
    food.add_child(CategoryNode("restaurants"))
    food.add_child(CategoryNode("wine"))
    food.add_child(CategoryNode("beer"))
    food.add_child(CategoryNode("cooking"))
    root.add_child(food)
    
    # Technology
    tech = CategoryNode("technology")
    tech.add_child(CategoryNode("programming"))
    tech.add_child(CategoryNode("startups"))
    tech.add_child(CategoryNode("data-science"))
    tech.add_child(CategoryNode("artificial-intelligence"))
    root.add_child(tech)
    
    # Business & Professional
    business = CategoryNode("business")
    business.add_child(CategoryNode("networking"))
    business.add_child(CategoryNode("marketing"))
    business.add_child(CategoryNode("entrepreneurship"))
    business.add_child(CategoryNode("finance"))
    root.add_child(business)
    
    # Education
    education = CategoryNode("education")
    education.add_child(CategoryNode("workshops"))
    education.add_child(CategoryNode("lectures"))
    education.add_child(CategoryNode("conferences"))
    education.add_child(CategoryNode("seminars"))
    root.add_child(education)
    
    # Community & Culture
    community = CategoryNode("community")
    community.add_child(CategoryNode("volunteering"))
    community.add_child(CategoryNode("social"))
    community.add_child(CategoryNode("charity"))
    community.add_child(CategoryNode("activism"))
    root.add_child(community)
    
    # Health & Wellness
    health = CategoryNode("health")
    health.add_child(CategoryNode("meditation"))
    health.add_child(CategoryNode("mental-health"))
    health.add_child(CategoryNode("nutrition"))
    health.add_child(CategoryNode("fitness"))
    root.add_child(health)
    
    return root.get_hierarchy()

def get_category_ancestors(category: str) -> List[str]:
    """Get list of ancestor categories for a given category."""
    hierarchy = get_category_hierarchy()
    
    def find_node(node: Dict[str, Any], target: str, path: List[str]) -> Optional[List[str]]:
        if node['name'] == target:
            return path
        for child in node.get('children', []):
            result = find_node(child, target, [node['name']] + path)
            if result is not None:
                return result
        return None
        
    return find_node(hierarchy, category, []) or []

def get_category_descendants(category: str) -> List[str]:
    """Get list of descendant categories for a given category."""
    hierarchy = get_category_hierarchy()
    
    def find_node(node: Dict[str, Any], target: str) -> Optional[Dict[str, Any]]:
        if node['name'] == target:
            return node
        for child in node.get('children', []):
            result = find_node(child, target)
            if result is not None:
                return result
        return None
        
    def get_descendants(node: Dict[str, Any]) -> List[str]:
        descendants = []
        for child in node.get('children', []):
            descendants.append(child['name'])
            descendants.extend(get_descendants(child))
        return descendants
        
    target_node = find_node(hierarchy, category)
    return get_descendants(target_node) if target_node else []

def is_category_ancestor(ancestor: str, descendant: str) -> bool:
    """Check if one category is an ancestor of another."""
    ancestors = get_category_ancestors(descendant)
    return ancestor in ancestors

def is_category_descendant(descendant: str, ancestor: str) -> bool:
    """Check if one category is a descendant of another."""
    descendants = get_category_descendants(ancestor)
    return descendant in descendants

def get_related_categories(category: str) -> List[str]:
    """Get list of related categories (siblings and cousins)."""
    ancestors = get_category_ancestors(category)
    if not ancestors:
        return []
        
    # Get siblings (categories with same parent)
    parent = ancestors[0]
    siblings = get_category_descendants(parent)
    
    # Get cousins (categories with same grandparent)
    cousins = []
    if len(ancestors) > 1:
        grandparent = ancestors[1]
        cousins = get_category_descendants(grandparent)
        
    # Combine and remove duplicates
    related = list(set(siblings + cousins))
    related.remove(category)  # Remove the category itself
    
    return related 