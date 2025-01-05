import pytest
from app.services.category_extraction import extract_categories_from_text
from app.schemas.event import Event
from app.schemas.category import CategoryHierarchy, CategoryNode

def test_extract_categories():
    """Test category extraction from text."""
    text = "Join us for a live music concert featuring jazz and blues bands"
    categories = extract_categories_from_text(text)
    assert "music" in categories
    assert "jazz" in categories
    assert "blues" in categories

def test_empty_text():
    """Test category extraction with empty text."""
    categories = extract_categories_from_text("")
    assert len(categories) == 0

def test_irrelevant_text():
    """Test category extraction with irrelevant text."""
    text = "This text contains no relevant category information"
    categories = extract_categories_from_text(text)
    assert len(categories) == 0

def test_multiple_mentions():
    """Test that repeated categories are only included once."""
    text = "Music event with music performances by musical artists"
    categories = extract_categories_from_text(text)
    assert len([c for c in categories if c == "music"]) == 1

def test_case_insensitive():
    """Test that category extraction is case insensitive."""
    text = "MUSIC event with Jazz and BLUES"
    categories = extract_categories_from_text(text)
    assert "music" in categories
    assert "jazz" in categories
    assert "blues" in categories 