import pytest
from app.services.category_hierarchy import (
    create_default_hierarchy,
    map_platform_categories,
    extract_categories
)
from app.schemas.event import Event
from app.schemas.category import CategoryNode, CategoryHierarchy
from typing import Dict, List, Set

def test_create_default_hierarchy():
    """Test creating default category hierarchy."""
    hierarchy = create_default_hierarchy()
    assert isinstance(hierarchy, CategoryHierarchy)
    assert "events" in hierarchy.nodes
    assert "music" in hierarchy.nodes
    assert "sports" in hierarchy.nodes

def test_map_platform_categories():
    """Test mapping platform-specific categories."""
    hierarchy = create_default_hierarchy()
    categories = ["MUSIC_EVENT", "SPORTS_EVENT"]
    mapped = map_platform_categories(categories, "meta", hierarchy)
    assert "music" in mapped
    assert "sports" in mapped
    assert "events" in mapped  # Parent category should be included

def test_category_hierarchy():
    """Test category hierarchy functionality."""
    hierarchy = CategoryHierarchy()
    
    # Add categories
    hierarchy.add_category("events")
    hierarchy.add_category("music", parent="events")
    hierarchy.add_category("jazz", parent="music")
    
    # Test relationships
    assert hierarchy.get_ancestors("jazz") == ["events", "music"]
    assert "jazz" in hierarchy.get_descendants("events")
    assert "jazz" in hierarchy.get_descendants("music") 