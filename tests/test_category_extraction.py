import pytest
from datetime import datetime
import spacy
from app.utils.schema import Event, Location, PriceInfo, EventAttributes, SourceInfo
from app.utils.category_extraction import (
    extract_categories_from_text,
    match_categories,
    extract_hierarchical_categories,
    get_category_hierarchy,
    enrich_event_categories
)

@pytest.fixture
def nlp():
    """Load spaCy model once for all tests."""
    return spacy.load("en_core_web_sm")

@pytest.fixture
def sample_event():
    """Create a sample event for testing."""
    return Event(
        event_id="1",
        title="Classical Orchestra Concert at Symphony Hall",
        description="Join us for an evening of classical masterpieces performed by the city orchestra. The program includes works by Mozart and Beethoven.",
        short_description="Classical concert featuring Mozart and Beethoven",
        start_datetime=datetime.now(),
        end_datetime=datetime.now(),
        location=Location(
            venue_name="Symphony Hall",
            address="123 Music St",
            city="Boston",
            state="MA",
            country="USA",
            coordinates={'lat': 42.3434, 'lng': -71.0851}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=50.0,
            max_price=150.0,
            price_tier="medium"
        ),
        categories=["concert"],  # Initial category
        source=SourceInfo(
            platform="eventbrite",
            url="https://example.com/1",
            last_updated=datetime.now()
        ),
        attributes=EventAttributes()
    )

def test_extract_categories_from_text(nlp):
    """Test keyword extraction from text."""
    text = "Classical orchestra concert featuring symphony performances"
    keywords = extract_categories_from_text(text, nlp)
    
    assert "concert" in keywords
    assert "orchestra" in keywords
    assert "symphony" in keywords
    
    # Test empty text
    assert extract_categories_from_text("", nlp) == set()
    
    # Test text without relevant keywords
    text = "The quick brown fox jumps over the lazy dog"
    keywords = extract_categories_from_text(text, nlp)
    assert "concert" not in keywords
    assert "music" not in keywords

def test_match_categories():
    """Test matching keywords to categories."""
    # Test music-related keywords
    keywords = {"concert", "performance", "stage", "band"}
    categories = match_categories(keywords)
    
    assert "concert" in categories
    assert "music" in categories
    assert categories["concert"] > 0.5  # High confidence for direct match
    
    # Test arts-related keywords
    keywords = {"exhibition", "gallery", "artist"}
    categories = match_categories(keywords)
    
    assert "exhibition" in categories
    assert "arts" in categories
    
    # Test low confidence filtering
    keywords = {"random", "words", "not", "related"}
    categories = match_categories(keywords, min_confidence=0.3)
    assert len(categories) == 0

def test_get_category_hierarchy():
    """Test retrieving category hierarchy."""
    # Test subcategory
    hierarchy = get_category_hierarchy("concert")
    assert hierarchy == ["music", "concert"]
    
    # Test root category
    hierarchy = get_category_hierarchy("music")
    assert hierarchy == ["music"]
    
    # Test invalid category
    hierarchy = get_category_hierarchy("invalid_category")
    assert hierarchy == []

def test_extract_hierarchical_categories(sample_event, nlp):
    """Test extracting categories with hierarchy."""
    categories = extract_hierarchical_categories(sample_event, nlp)
    
    # Should find music-related categories
    assert "music" in categories
    assert "concert" in categories
    assert "classical" in categories
    
    # Check confidence scores
    assert categories["concert"] > 0.5  # High confidence due to existing category
    assert categories["music"] > 0.4  # Good confidence as parent category

def test_enrich_event_categories(sample_event, nlp):
    """Test enriching event with hierarchical categories."""
    enriched = enrich_event_categories(sample_event, nlp)
    
    # Check enriched categories
    assert "music" in enriched.categories  # Parent category
    assert "concert" in enriched.categories  # Original category
    assert "classical" in enriched.categories  # Extracted from content
    
    # Test event with no initial categories
    sample_event.categories = []
    enriched = enrich_event_categories(sample_event, nlp)
    assert len(enriched.categories) > 0  # Should still find categories
    assert "music" in enriched.categories
    
    # Test event with minimal text
    sample_event.title = "Event"
    sample_event.description = ""
    enriched = enrich_event_categories(sample_event, nlp)
    assert len(enriched.categories) == 0  # Not enough info to categorize 