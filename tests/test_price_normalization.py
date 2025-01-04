import pytest
from datetime import datetime
from app.utils.schema import Event, Location, PriceInfo, EventAttributes, SourceInfo
from app.utils.price_normalization import (
    normalize_price,
    calculate_price_tier,
    get_category_price_statistics,
    normalize_event_price,
    batch_normalize_prices,
    PriceStatistics
)

@pytest.fixture
def sample_events():
    """Create a list of sample events with various prices and currencies."""
    base_time = datetime.now()
    events = []
    
    # Event 1: USD Premium Concert
    events.append(Event(
        event_id="1",
        title="Premium Concert",
        description="High-end concert",
        start_datetime=base_time,
        end_datetime=base_time,
        location=Location(
            venue_name="Concert Hall",
            address="123 Main St",
            city="New York",
            state="NY",
            country="USA",
            coordinates={'lat': 0.0, 'lng': 0.0}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=150.0,
            max_price=300.0,
            price_tier="premium"
        ),
        categories=["music", "concert"],
        source=SourceInfo(
            platform="test",
            url="https://example.com/1",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    # Event 2: EUR Budget Theater
    events.append(Event(
        event_id="2",
        title="Local Theater",
        description="Community theater show",
        start_datetime=base_time,
        end_datetime=base_time,
        location=Location(
            venue_name="Community Theater",
            address="456 High St",
            city="Berlin",
            state="",
            country="Germany",
            coordinates={'lat': 0.0, 'lng': 0.0}
        ),
        price_info=PriceInfo(
            currency="EUR",
            min_price=15.0,
            max_price=25.0,
            price_tier="budget"
        ),
        categories=["theater", "arts"],
        source=SourceInfo(
            platform="test",
            url="https://example.com/2",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    # Event 3: Free Community Event
    events.append(Event(
        event_id="3",
        title="Community Festival",
        description="Local festival",
        start_datetime=base_time,
        end_datetime=base_time,
        location=Location(
            venue_name="City Park",
            address="789 Park Rd",
            city="Chicago",
            state="IL",
            country="USA",
            coordinates={'lat': 0.0, 'lng': 0.0}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=0.0,
            max_price=0.0,
            price_tier="free"
        ),
        categories=["festival", "community"],
        source=SourceInfo(
            platform="test",
            url="https://example.com/3",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    return events

def test_normalize_price():
    """Test currency conversion functionality."""
    # Test USD to USD (no conversion)
    assert normalize_price(100.0, "USD") == 100.0
    
    # Test EUR to USD
    assert normalize_price(100.0, "EUR") == 109.0
    
    # Test GBP to USD
    assert normalize_price(100.0, "GBP") == 127.0
    
    # Test unknown currency (should return original amount)
    assert normalize_price(100.0, "XYZ") == 100.0

def test_calculate_price_tier():
    """Test price tier calculation."""
    # Test with no category stats
    assert calculate_price_tier(0.0) == "free"
    assert calculate_price_tier(25.0) == "budget"  # Well within budget range
    assert calculate_price_tier(75.0) == "medium"  # Middle of medium range
    assert calculate_price_tier(150.0) == "premium"  # Above premium threshold
    
    # Test edge cases
    assert calculate_price_tier(40.0) == "budget"  # Upper limit of budget
    assert calculate_price_tier(40.1) == "medium"  # Just into medium
    assert calculate_price_tier(100.0) == "medium"  # Upper limit of medium
    assert calculate_price_tier(100.1) == "premium"  # Just into premium
    
    # Test with category stats
    stats = PriceStatistics(
        mean=50.0,
        median=45.0,
        percentile_25=25.0,
        percentile_75=75.0,
        min_price=10.0,
        max_price=100.0,
        sample_size=100
    )
    
    assert calculate_price_tier(20.0, stats) == "budget"
    assert calculate_price_tier(50.0, stats) == "medium"
    assert calculate_price_tier(80.0, stats) == "premium"

def test_get_category_price_statistics(sample_events):
    """Test calculation of category price statistics."""
    stats = get_category_price_statistics(sample_events, "music")
    
    assert stats.sample_size == 1
    assert stats.min_price == 300.0
    assert stats.max_price == 300.0
    assert stats.mean == 300.0
    
    # Test empty category
    empty_stats = get_category_price_statistics(sample_events, "sports")
    assert empty_stats.sample_size == 0
    assert empty_stats.mean == 0

def test_normalize_event_price(sample_events):
    """Test price normalization for a single event."""
    # Test EUR event
    event = sample_events[1]  # Local Theater event in EUR
    normalized = normalize_event_price(event)
    
    assert normalized.price_info.currency == "USD"
    assert normalized.price_info.min_price == pytest.approx(16.35, 0.01)  # 15 EUR to USD
    assert normalized.price_info.max_price == pytest.approx(27.25, 0.01)  # 25 EUR to USD
    assert normalized.price_info.price_tier == "budget"

def test_batch_normalize_prices(sample_events):
    """Test batch price normalization."""
    # Print initial event details
    print("\nInitial events:")
    for event in sample_events:
        print(f"\nEvent: {event.title}")
        print(f"Categories: {event.categories}")
        print(f"Price: {event.price_info.max_price} {event.price_info.currency}")
        print(f"Initial tier: {event.price_info.price_tier}")
    
    normalized_events = batch_normalize_prices(sample_events)
    
    # Print normalized event details
    print("\nNormalized events:")
    for event in normalized_events:
        print(f"\nEvent: {event.title}")
        print(f"Categories: {event.categories}")
        print(f"Price: {event.price_info.max_price} {event.price_info.currency}")
        print(f"Final tier: {event.price_info.price_tier}")
    
    assert len(normalized_events) == len(sample_events)
    
    # Check that all events now use USD
    assert all(event.price_info.currency == "USD" for event in normalized_events)
    
    # Check specific normalizations
    premium_concert = next(e for e in normalized_events if e.title == "Premium Concert")
    assert premium_concert.price_info.max_price == 300.0  # USD stays same
    assert premium_concert.price_info.price_tier == "premium"  # Should be premium (>$100)
    
    local_theater = next(e for e in normalized_events if e.title == "Local Theater")
    assert local_theater.price_info.max_price == pytest.approx(27.25, 0.01)  # EUR to USD
    assert local_theater.price_info.price_tier == "budget"  # Should be budget (<$40)
    
    community_festival = next(e for e in normalized_events if e.title == "Community Festival")
    assert community_festival.price_info.max_price == 0.0
    assert community_festival.price_info.price_tier == "free"  # Should be free ($0) 