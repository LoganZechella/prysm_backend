import pytest
from datetime import datetime
from app.utils.category_hierarchy import (
    create_default_hierarchy,
    extract_categories,
    map_platform_categories
)
from app.utils.schema import (
    Event,
    CategoryHierarchy,
    Location,
    SourceInfo
)

def test_create_default_hierarchy():
    """Test creation of default category hierarchy"""
    hierarchy = create_default_hierarchy()
    
    # Test root category
    assert "events" in hierarchy.nodes
    assert hierarchy.nodes["events"].parent is None
    
    # Test some child categories
    assert "arts_culture" in hierarchy.nodes
    assert hierarchy.nodes["arts_culture"].parent == "events"
    
    assert "visual_arts" in hierarchy.nodes
    assert hierarchy.nodes["visual_arts"].parent == "arts_culture"
    
    # Test platform mappings
    assert "eventbrite" in hierarchy.nodes["music"].platform_mappings
    assert "meta" in hierarchy.nodes["music"].platform_mappings
    assert "Music" in hierarchy.nodes["music"].platform_mappings["eventbrite"]
    assert "MUSIC_EVENT" in hierarchy.nodes["music"].platform_mappings["meta"]

def test_category_hierarchy_operations():
    """Test CategoryHierarchy class operations"""
    hierarchy = CategoryHierarchy()
    
    # Test adding categories
    hierarchy.add_category("root")
    hierarchy.add_category("child", parent="root")
    hierarchy.add_category("grandchild", parent="child")
    
    assert "root" in hierarchy.nodes
    assert "child" in hierarchy.nodes
    assert "grandchild" in hierarchy.nodes
    
    # Test parent-child relationships
    assert hierarchy.nodes["child"].parent == "root"
    assert hierarchy.nodes["grandchild"].parent == "child"
    assert "child" in hierarchy.nodes["root"].children
    assert "grandchild" in hierarchy.nodes["child"].children
    
    # Test ancestor retrieval
    ancestors = hierarchy.get_ancestors("grandchild")
    assert ancestors == ["child", "root"]
    
    # Test descendant retrieval
    descendants = hierarchy.get_descendants("root")
    assert set(descendants) == {"child", "grandchild"}

def test_extract_categories():
    """Test category extraction from text"""
    hierarchy = create_default_hierarchy()
    
    # Test music event
    text = "Join us for an amazing live music concert featuring electronic music and DJ performances!"
    categories = extract_categories(text, hierarchy)
    
    assert "music" in categories
    assert "live_music" in categories
    assert "electronic_music" in categories
    assert "events" in categories  # Parent category should be included
    
    # Test mixed event
    text = "Art gallery exhibition with live music and food tasting"
    categories = extract_categories(text, hierarchy)
    
    assert "visual_arts" in categories
    assert "arts_culture" in categories
    assert "live_music" in categories
    assert "food_tasting" in categories

def test_map_platform_categories():
    """Test mapping of platform-specific categories"""
    hierarchy = create_default_hierarchy()
    
    # Test Eventbrite categories
    eventbrite_cats = ["Music", "Food & Drink", "Visual Arts"]
    mapped = map_platform_categories("eventbrite", eventbrite_cats, hierarchy)
    
    assert "music" in mapped
    assert "food_drink" in mapped
    assert "visual_arts" in mapped
    assert "events" in mapped  # Root category should be included
    
    # Test Meta categories
    meta_cats = ["MUSIC_EVENT", "FOOD_EVENT", "ART_EVENT"]
    mapped = map_platform_categories("meta", meta_cats, hierarchy)
    
    assert "music" in mapped
    assert "food_drink" in mapped
    assert "visual_arts" in mapped

def test_event_category_integration():
    """Test integration with Event model"""
    hierarchy = create_default_hierarchy()
    event = Event(
        event_id="test123",
        title="Art Gallery Opening with Live Music",
        description="Join us for an art gallery opening featuring live music performances",
        start_datetime=datetime(2024, 1, 1, 19, 0),
        location=Location(
            venue_name="Test Gallery",
            address="123 Test St",
            city="Test City",
            state="TC",
            country="Test Country",
            coordinates={"lat": 0.0, "lng": 0.0}
        ),
        source=SourceInfo(
            platform="eventbrite",
            url="https://test.com",
            last_updated=datetime(2024, 1, 1)
        )
    )
    
    # Add some categories
    event.add_category("visual_arts", hierarchy)
    event.add_category("live_music", hierarchy)
    
    # Test direct categories
    assert "visual_arts" in event.categories
    assert "live_music" in event.categories
    
    # Test hierarchical relationships
    assert "arts_culture" in event.category_hierarchy["visual_arts"]
    assert "events" in event.category_hierarchy["visual_arts"]
    assert "music" in event.category_hierarchy["live_music"]
    
    # Test getting all categories including ancestors
    all_cats = event.get_all_categories()
    assert "visual_arts" in all_cats
    assert "live_music" in all_cats
    assert "arts_culture" in all_cats
    assert "music" in all_cats
    assert "events" in all_cats 