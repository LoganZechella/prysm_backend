import pytest
from datetime import datetime, timedelta
from app.schemas.event import EventBase, Location, PriceInfo, EventAttributes, SourceInfo
from app.utils.deduplication import (
    calculate_title_similarity,
    calculate_time_proximity,
    calculate_category_overlap,
    calculate_description_similarity,
    calculate_duplicate_score,
    find_duplicate_events,
    merge_duplicate_events,
    deduplicate_events
)

@pytest.fixture
def sample_events():
    """Create a list of sample events including duplicates."""
    base_time = datetime.now()
    events = []
    
    # Original event
    events.append(EventBase(
        event_id="1",
        title="Summer Music Festival 2024",
        description="Annual summer music festival featuring local and international artists",
        start_datetime=base_time,
        end_datetime=base_time + timedelta(hours=6),
        location=Location(
            venue_name="Central Park",
            address="123 Park Ave",
            city="New York",
            state="NY",
            country="USA",
            coordinates={'lat': 40.7829, 'lng': -73.9654}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=50.0,
            max_price=150.0,
            price_tier="medium"
        ),
        categories=["music", "festival", "outdoor"],
        source=SourceInfo(
            platform="eventbrite",
            url="https://example.com/1",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    # Duplicate event with slightly different details
    events.append(EventBase(
        event_id="2",
        title="Summer Music Fest '24",  # Slightly different title
        description="The biggest summer music festival in NYC with amazing artists",  # Different description
        start_datetime=base_time + timedelta(minutes=30),  # Slightly different time
        end_datetime=base_time + timedelta(hours=6),
        location=Location(
            venue_name="Central Park Main Stage",  # More specific venue
            address="123 Park Avenue",
            city="New York",
            state="NY",
            country="USA",
            coordinates={'lat': 40.7830, 'lng': -73.9653}  # Slightly different coordinates
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=45.0,  # Slightly different price
            max_price=155.0,
            price_tier="medium"
        ),
        categories=["music", "festival", "entertainment"],  # Slightly different categories
        source=SourceInfo(
            platform="meta",  # Different source
            url="https://example.com/2",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    # Clearly different event
    events.append(EventBase(
        event_id="3",
        title="Winter Art Exhibition",
        description="Showcase of local artists' winter-themed works",
        start_datetime=base_time + timedelta(days=30),
        end_datetime=base_time + timedelta(days=30, hours=4),
        location=Location(
            venue_name="City Gallery",
            address="456 Art St",
            city="New York",
            state="NY",
            country="USA",
            coordinates={'lat': 40.7500, 'lng': -73.9000}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=0.0,
            max_price=0.0,
            price_tier="free"
        ),
        categories=["art", "exhibition", "indoor"],
        source=SourceInfo(
            platform="eventbrite",
            url="https://example.com/3",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    return events

def test_calculate_title_similarity():
    """Test title similarity calculation."""
    # Exact match
    assert calculate_title_similarity("Summer Festival", "Summer Festival") == 1.0
    
    # Close match
    assert calculate_title_similarity("Summer Festival 2024", "Summer Fest '24") > 0.6
    
    # Different titles
    assert calculate_title_similarity("Summer Festival", "Winter Exhibition") < 0.4
    
    # Case insensitive
    assert calculate_title_similarity("SUMMER FESTIVAL", "summer festival") == 1.0

def test_calculate_time_proximity():
    """Test time proximity calculation."""
    base_time = datetime.now()
    
    # Same time
    assert calculate_time_proximity(base_time, base_time) == 1.0
    
    # Within threshold (12 hours)
    score = calculate_time_proximity(
        base_time,
        base_time + timedelta(hours=12)
    )
    assert score == 0.5  # Exactly half the time window
    
    # Very close times should get a boost
    score = calculate_time_proximity(
        base_time,
        base_time + timedelta(hours=2)
    )
    assert score > 0.9
    
    # Beyond threshold
    assert calculate_time_proximity(
        base_time,
        base_time + timedelta(days=2)
    ) == 0.0

def test_calculate_category_overlap():
    """Test category overlap calculation."""
    # Exact match
    assert calculate_category_overlap(
        ["music", "festival"],
        ["music", "festival"]
    ) == 1.0
    
    # Partial overlap
    assert calculate_category_overlap(
        ["music", "festival", "outdoor"],
        ["music", "festival", "entertainment"]
    ) == 0.5  # 2 common out of 4 unique
    
    # No overlap
    assert calculate_category_overlap(
        ["music", "festival"],
        ["art", "exhibition"]
    ) == 0.0
    
    # Empty categories
    assert calculate_category_overlap([], ["music"]) == 0.0
    assert calculate_category_overlap(["music"], []) == 0.0

def test_calculate_description_similarity():
    """Test description similarity calculation."""
    desc1 = "Summer music festival with great artists"
    desc2 = "Summer music festival featuring amazing artists"
    desc3 = "Winter art exhibition showcase"
    
    # Similar descriptions
    assert calculate_description_similarity(desc1, desc2) > 0.7
    
    # Different descriptions
    assert calculate_description_similarity(desc1, desc3) < 0.3
    
    # Empty descriptions
    assert calculate_description_similarity("", desc1) == 0.0
    assert calculate_description_similarity(desc1, "") == 0.0
    assert calculate_description_similarity("", "") == 0.0

def test_calculate_duplicate_score(sample_events):
    """Test comprehensive duplicate score calculation."""
    event1 = sample_events[0]  # Original event
    event2 = sample_events[1]  # Similar event
    event3 = sample_events[2]  # Different event
    
    # Score between similar events
    score1 = calculate_duplicate_score(event1, event2)
    assert score1.combined_score > 0.75  # Adjusted threshold
    assert score1.title_similarity > 0.6
    assert score1.time_proximity > 0.9
    assert score1.location_proximity > 0.9
    assert score1.category_overlap >= 0.5
    
    # Score between different events
    score2 = calculate_duplicate_score(event1, event3)
    assert score2.combined_score < 0.3

def test_find_duplicate_events(sample_events):
    """Test finding duplicate events."""
    duplicates = find_duplicate_events(sample_events, threshold=0.75)  # Adjusted threshold
    
    # Should find one pair of duplicates
    assert len(duplicates) == 1
    
    # Check the duplicate pair
    event1, event2, score = duplicates[0]
    assert score > 0.75
    assert {event1.event_id, event2.event_id} == {"1", "2"}

def test_merge_duplicate_events(sample_events):
    """Test merging duplicate events."""
    event1 = sample_events[0]
    event2 = sample_events[1]
    
    merged = merge_duplicate_events(event1, event2)
    
    # Check merged event properties
    assert merged.source.platform == "eventbrite+meta"
    assert len(merged.categories) == 4  # Combined unique categories
    assert merged.price_info.min_price == 45.0  # Lower of the two
    assert merged.price_info.max_price == 155.0  # Higher of the two

def test_deduplicate_events(sample_events):
    """Test full deduplication process."""
    deduplicated = deduplicate_events(sample_events, threshold=0.75)  # Adjusted threshold
    
    # Should have two events after deduplication
    assert len(deduplicated) == 2
    
    # Check that we have the merged event and the different event
    titles = {event.title for event in deduplicated}
    assert "Winter Art Exhibition" in titles  # The clearly different event
    
    # Check that the merged event has combined information
    merged = next(e for e in deduplicated if e.title != "Winter Art Exhibition")
    assert "eventbrite" in merged.source.platform and "meta" in merged.source.platform 