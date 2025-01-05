import pytest
from datetime import datetime, timedelta
from app.services.data_quality import (
    validate_event,
    check_required_fields,
    validate_coordinates,
    validate_dates,
    validate_prices
)
from app.schemas.event import Event, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata

def test_validate_event():
    """Test event validation."""
    now = datetime.now()
    # Test valid event
    event = Event(
        event_id="1",
        title="Test Event",
        description="A test event",
        start_datetime=now,
        location=Location(
            venue_name="Test Venue",
            city="Test City",
            coordinates={"lat": 37.7749, "lon": -122.4194}
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
    
    assert validate_event(event) is True
    
    # Test invalid coordinates
    event.location.coordinates = {"lat": 100.0, "lon": 200.0}
    assert validate_event(event) is False
    
    # Test invalid dates
    event.end_datetime = now - timedelta(days=1)  # End before start
    assert validate_event(event) is False
    
    # Test invalid prices
    event.price_info = PriceInfo(
        min_price=-10.0,  # Negative price
        max_price=50.0,
        currency="USD",
        price_tier="medium"
    )
    assert validate_event(event) is False

def test_check_required_fields():
    """Test required field validation."""
    now = datetime.now()
    event = Event(
        event_id="1",
        title="Test Event",
        description="A test event",
        start_datetime=now,
        location=Location(
            venue_name="Test Venue",
            city="Test City",
            coordinates={"lat": 37.7749, "lon": -122.4194}
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
    
    assert check_required_fields(event) is True
    
    # Test missing title
    event.title = ""
    assert check_required_fields(event) is False
    
    # Test missing description
    event.description = ""
    assert check_required_fields(event) is False

def test_validate_coordinates():
    """Test coordinate validation."""
    # Test valid coordinates
    assert validate_coordinates({"lat": 37.7749, "lon": -122.4194}) is True
    
    # Test invalid latitude
    assert validate_coordinates({"lat": 100.0, "lon": -122.4194}) is False
    
    # Test invalid longitude
    assert validate_coordinates({"lat": 37.7749, "lon": 200.0}) is False
    
    # Test missing coordinates
    assert validate_coordinates({}) is False

def test_validate_dates():
    """Test date validation."""
    now = datetime.now()
    
    # Test valid dates
    assert validate_dates(now, now + timedelta(hours=1)) is True
    
    # Test end before start
    assert validate_dates(now, now - timedelta(hours=1)) is False
    
    # Test missing end date
    assert validate_dates(now, None) is True

def test_validate_prices():
    """Test price validation."""
    # Test valid prices
    assert validate_prices(PriceInfo(
        min_price=10.0,
        max_price=50.0,
        currency="USD",
        price_tier="medium"
    )) is True
    
    # Test negative price
    assert validate_prices(PriceInfo(
        min_price=-10.0,
        max_price=50.0,
        currency="USD",
        price_tier="medium"
    )) is False
    
    # Test min > max
    assert validate_prices(PriceInfo(
        min_price=60.0,
        max_price=50.0,
        currency="USD",
        price_tier="medium"
    )) is False
    
    # Test invalid currency
    assert validate_prices(PriceInfo(
        min_price=10.0,
        max_price=50.0,
        currency="INVALID",
        price_tier="medium"
    )) is False 