from typing import Dict, Any, Optional
from app.services.location_recommendations import LocationService

# Create a singleton instance of LocationService
_location_service = LocationService()

def geocode_address(
    address: str,
    city: str,
    state: str,
    country: str = "US"
) -> Optional[Dict[str, float]]:
    """Compatibility function that uses LocationService."""
    return _location_service.geocode_venue(address, city, state, country)

def calculate_distance(point1: Dict[str, float], point2: Dict[str, float]) -> float:
    """Compatibility function that uses LocationService."""
    return _location_service.calculate_distance(point1, point2)

def filter_events_by_distance(events, user_location, max_distance):
    """Compatibility function that uses LocationService."""
    return _location_service.filter_by_distance(events, user_location, max_distance)

def sort_events_by_distance(events, user_location):
    """Compatibility function that uses LocationService."""
    return _location_service.sort_by_distance(events, user_location) 