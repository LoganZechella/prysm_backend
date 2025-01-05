import pytest
from datetime import datetime
from app.services.price_normalization import (
    normalize_price,
    calculate_price_tier,
    enrich_event_prices
)
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

def test_normalize_price():
    """Test price normalization."""
    # Test USD prices
    assert normalize_price(10.0, "USD") == 10.0
    assert normalize_price(100.0, "USD") == 100.0
    
    # Test EUR prices
    assert 110.0 <= normalize_price(100.0, "EUR") <= 130.0
    
    # Test GBP prices
    assert 120.0 <= normalize_price(100.0, "GBP") <= 140.0
    
    # Test invalid currency
    with pytest.raises(ValueError):
        normalize_price(100.0, "INVALID")

def test_calculate_price_tier():
    """Test price tier calculation."""
    assert calculate_price_tier(0.0) == "free"
    assert calculate_price_tier(10.0) == "low"
    assert calculate_price_tier(30.0) == "medium"
    assert calculate_price_tier(100.0) == "high"

def test_enrich_event_prices():
    """Test event price enrichment."""
    now = datetime.now()
    event = Event(
        event_id="1",
        title="Test Event",
        description="A test event",
        start_datetime=now,
        location=Location(
            venue_name="Test Venue",
            city="Test City",
            coordinates={"lat": 0.0, "lon": 0.0}
        ),
        categories=["test"],
        attributes=EventAttributes(
            indoor_outdoor="indoor",
            age_restriction="all"
        ),
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=now
        ),
        metadata=EventMetadata(
            popularity_score=0.8
        )
    )
    
    # Test with USD prices
    event.price_info = PriceInfo(
        min_price=10.0,
        max_price=50.0,
        currency="USD",
        price_tier="medium"  # Initial tier will be recalculated
    )
    enriched = enrich_event_prices(event)
    assert enriched.price_info is not None
    assert enriched.price_info.min_price == 10.0
    assert enriched.price_info.max_price == 50.0
    assert enriched.price_info.price_tier == "medium"
    
    # Test with EUR prices
    event.price_info = PriceInfo(
        min_price=10.0,
        max_price=50.0,
        currency="EUR",
        price_tier="medium"  # Initial tier will be recalculated
    )
    enriched = enrich_event_prices(event)
    assert enriched.price_info is not None
    assert enriched.price_info.min_price > 10.0  # Should be converted to USD
    assert enriched.price_info.max_price > 50.0  # Should be converted to USD
    assert enriched.price_info.price_tier in {"medium", "high"}  # Depends on conversion rate
    
    # Test free event
    event.price_info = PriceInfo(
        min_price=0.0,
        max_price=0.0,
        currency="USD",
        price_tier="free"
    )
    enriched = enrich_event_prices(event)
    assert enriched.price_info is not None
    assert enriched.price_info.min_price == 0.0
    assert enriched.price_info.max_price == 0.0
    assert enriched.price_info.price_tier == "free" 