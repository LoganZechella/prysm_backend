import pytest
from datetime import datetime, timedelta
from app.services.data_quality import (
    validate_event,
    check_required_fields,
    validate_coordinates,
    validate_dates,
    validate_prices
)
from app.schemas.event import EventBase, PriceInfo
from app.schemas.validation import LocationBase
from app.schemas.source import SourceInfo
from app.schemas.metadata import EventMetadata

def test_validate_event():
    """Test event validation."""
    now = datetime.now()
    # Test valid event
    event = EventBase(
        platform_id="test-1",
        title="Test Event",
        description="A test event",
        start_datetime=now,
        url="http://test.com/event/1",
        platform="test",
        venue_name="Test Venue",
        venue_city="Test City",
        venue_country="Test Country",
        venue_lat=37.7749,
        venue_lon=-122.4194,
        categories=["test"],
        is_online=False,
        rsvp_count=0,
        price_info={
            "currency": "USD",
            "min_price": 10.0,
            "max_price": 50.0,
            "tier": "medium"
        }
    )
    
    assert validate_event(event) is True
    
    # Test invalid coordinates
    event.venue_lat = 100.0
    event.venue_lon = 200.0
    assert validate_event(event) is False
    
    # Test invalid dates
    event.end_datetime = now - timedelta(days=1)  # End before start
    assert validate_event(event) is False
    
    # Test invalid prices
    event.price_info = {
        "currency": "USD",
        "min_price": -10.0,  # Negative price
        "max_price": 50.0,
        "tier": "medium"
    }
    assert validate_event(event) is False

def test_check_required_fields():
    """Test required field validation."""
    now = datetime.now()
    event = EventBase(
        platform_id="test-1",
        title="Test Event",
        description="A test event",
        start_datetime=now,
        url="http://test.com/event/1",
        platform="test",
        venue_name="Test Venue",
        venue_city="Test City",
        venue_country="Test Country",
        venue_lat=37.7749,
        venue_lon=-122.4194,
        categories=["test"],
        is_online=False,
        rsvp_count=0
    )
    
    assert check_required_fields(event) is True
    
    # Test missing title
    event.title = ""
    assert check_required_fields(event) is False
    
    # Test missing venue name
    event.venue_name = ""
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
        currency="USD",
        min_price=10.0,
        max_price=50.0
    )) is True
    
    # Test negative price
    assert validate_prices(PriceInfo(
        currency="USD",
        min_price=-10.0,
        max_price=50.0
    )) is False
    
    # Test invalid currency
    assert validate_prices(PriceInfo(
        currency="INVALID",
        min_price=10.0,
        max_price=50.0
    )) is False 