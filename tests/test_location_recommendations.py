import pytest
from datetime import datetime
from typing import Dict, Any, List
from app.services.location_recommendations import LocationService
from app.schemas.event import EventBase, Location, PriceInfo, SourceInfo, EventAttributes, EventMetadata
from app.models.event import EventModel as Event

@pytest.fixture
def location_service():
    return LocationService()

def test_calculate_distance(location_service):
    """Test distance calculation between two points."""
    point1 = {"lat": 40.7128, "lon": -74.0060}  # New York
    point2 = {"lat": 34.0522, "lon": -118.2437}  # Los Angeles
    
    # Distance should be roughly 3935 km
    distance = location_service.calculate_distance(point1, point2)
    assert 3900 < distance < 4000

def test_filter_by_distance(location_service):
    """Test filtering events by distance."""
    user_location = {"lat": 40.7128, "lon": -74.0060}  # New York
    max_distance = 100  # km
    
    events = [
        {
            "title": "Event 1",
            "venue": {
                "lat": 40.7589, 
                "lon": -73.9851
            }  # ~5km from user
        },
        {
            "title": "Event 2",
            "venue": {
                "lat": 40.7829,
                "lon": -73.9654
            }  # ~8km from user
        },
        {
            "title": "Event 3",
            "venue": {
                "lat": 42.3601,
                "lon": -71.0589
            }  # ~300km from user
        }
    ]
    
    filtered = location_service.filter_by_distance(events, user_location, max_distance)
    assert len(filtered) == 2
    assert filtered[0]["title"] == "Event 1"
    assert filtered[1]["title"] == "Event 2"
    
    # Test with different coordinate formats
    events = [
        {
            "title": "Event 1",
            "venue_lat": 40.7589,
            "venue_lon": -73.9851
        },
        {
            "title": "Event 2",
            "venue": {
                "lat": 40.7829,
                "lng": -73.9654
            }
        }
    ]
    
    filtered = location_service.filter_by_distance(events, user_location, max_distance)
    assert len(filtered) == 2

def test_sort_by_distance(location_service):
    """Test sorting events by distance."""
    user_location = {"lat": 40.7128, "lon": -74.0060}  # New York
    
    events = [
        {
            "title": "Far Event",
            "venue": {
                "lat": 42.3601,
                "lon": -71.0589
            }  # Boston, ~300km
        },
        {
            "title": "Close Event",
            "venue": {
                "lat": 40.7589,
                "lon": -73.9851
            }  # ~5km
        },
        {
            "title": "Medium Event",
            "venue": {
                "lat": 40.7829,
                "lon": -73.9654
            }  # ~8km
        }
    ]
    
    sorted_events = location_service.sort_by_distance(events, user_location)
    assert len(sorted_events) == 3
    assert sorted_events[0]["title"] == "Close Event"
    assert sorted_events[1]["title"] == "Medium Event"
    assert sorted_events[2]["title"] == "Far Event"
    
    # Test with different coordinate formats
    events = [
        {
            "title": "Event 1",
            "venue_lat": 40.7589,
            "venue_lon": -73.9851
        },
        {
            "title": "Event 2",
            "venue": {
                "lat": 40.7829,
                "lng": -73.9654
            }
        }
    ]
    
    sorted_events = location_service.sort_by_distance(events, user_location)
    assert len(sorted_events) == 2

def test_geocode_venue(location_service):
    """Test venue geocoding."""
    result = location_service.geocode_venue(
        venue_name="Empire State Building",
        city="New York",
        state="NY"
    )
    
    assert result is not None
    assert "lat" in result
    assert "lon" in result
    assert "lng" in result  # Test lng compatibility
    assert 40.7 < result["lat"] < 40.8  # Roughly correct latitude
    assert -74.0 < result["lon"] < -73.9  # Roughly correct longitude
    assert result["lon"] == result["lng"]  # lon and lng should match

def test_get_coordinates(location_service):
    """Test coordinate extraction and geocoding."""
    # Test with lat/lon
    loc1 = {"lat": 40.7128, "lon": -74.0060}
    coords1 = location_service.get_coordinates(loc1)
    assert coords1 is not None
    assert coords1["lat"] == 40.7128
    assert coords1["lon"] == -74.0060
    assert coords1["lng"] == -74.0060
    
    # Test with lat/lng
    loc2 = {"lat": 40.7128, "lng": -74.0060}
    coords2 = location_service.get_coordinates(loc2)
    assert coords2 is not None
    assert coords2["lat"] == 40.7128
    assert coords2["lon"] == -74.0060
    assert coords2["lng"] == -74.0060
    
    # Test with address
    loc3 = {"address": "Empire State Building, New York, NY"}
    coords3 = location_service.get_coordinates(loc3)
    assert coords3 is not None
    assert 40.7 < coords3["lat"] < 40.8
    assert -74.0 < coords3["lon"] < -73.9
    assert coords3["lon"] == coords3["lng"]

def test_filter_events_with_models(location_service):
    """Test filtering events using Event models."""
    now = datetime.now()
    # Create test events
    events = [
        Event(
            title="SF Event",
            description="Event in San Francisco",
            start_datetime=now,
            venue_name="SF Venue",
            venue_lat=37.7749,
            venue_lon=-122.4194,
            categories=["test"],
            platform="test",
            url="http://test.com"
        ),
        Event(
            title="LA Event",
            description="Event in Los Angeles",
            start_datetime=now,
            venue_name="LA Venue",
            venue_lat=34.0522,
            venue_lon=-118.2437,
            categories=["test"],
            platform="test",
            url="http://test.com"
        )
    ]
    
    # Filter from SF with 100km radius
    user_location = {"lat": 37.7749, "lon": -122.4194}
    max_distance = 100
    
    filtered = location_service.filter_by_distance(events, user_location, max_distance)
    assert len(filtered) == 1
    assert filtered[0].title == "SF Event"

def test_sort_events_with_models(location_service):
    """Test sorting events using Event models."""
    now = datetime.now()
    # Create test events
    events = [
        Event(
            title="LA Event",
            description="Event in Los Angeles",
            start_datetime=now,
            venue_name="LA Venue",
            venue_lat=34.0522,
            venue_lon=-118.2437,
            categories=["test"],
            platform="test",
            url="http://test.com"
        ),
        Event(
            title="SF Event",
            description="Event in San Francisco",
            start_datetime=now,
            venue_name="SF Venue",
            venue_lat=37.7749,
            venue_lon=-122.4194,
            categories=["test"],
            platform="test",
            url="http://test.com"
        )
    ]
    
    # Sort from SF
    user_location = {"lat": 37.7749, "lon": -122.4194}
    
    sorted_events = location_service.sort_by_distance(events, user_location)
    assert len(sorted_events) == 2
    assert sorted_events[0].title == "SF Event"  # SF event should be first
    assert sorted_events[1].title == "LA Event"  # LA event should be second 