import pytest
from app.services.category_hierarchy import (
    create_default_hierarchy,
    map_platform_categories,
    CategoryHierarchy,
    CategoryNode
)
from app.schemas.event import Event
from app.schemas.category import CategoryHierarchy, CategoryNode

def test_create_default_hierarchy():
    """Test creating default category hierarchy."""
    hierarchy = create_default_hierarchy()
    assert isinstance(hierarchy, CategoryHierarchy)
    assert "events" in hierarchy.nodes
    assert "music" in hierarchy.nodes
    assert "sports" in hierarchy.nodes

def test_map_platform_categories():
    """Test mapping platform-specific categories."""
    categories = ["MUSIC_EVENT", "SPORTS_EVENT"]
    mapped = map_platform_categories(categories, "facebook")
    assert "music" in mapped
    assert "sports" in mapped

def test_category_hierarchy():
    """Test category hierarchy functionality."""
    hierarchy = CategoryHierarchy()
    
    # Add categories
    hierarchy.add_category("events")
    hierarchy.add_category("music", parent="events")
    hierarchy.add_category("jazz", parent="music")
    
    # Test relationships
    assert hierarchy.get_ancestors("jazz") == ["events", "music"]
    assert hierarchy.get_ancestors("music") == ["events"]
    assert hierarchy.get_ancestors("events") == [] 