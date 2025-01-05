from typing import List, Dict, Any
from math import radians, sin, cos, sqrt, atan2
from app.schemas.event import Event

def calculate_distance(point1: Dict[str, float], point2: Dict[str, float]) -> float:
    """
    Calculate the distance between two points using the Haversine formula.
    
    Args:
        point1: Dictionary with lat/lon coordinates
        point2: Dictionary with lat/lon coordinates
        
    Returns:
        Distance in kilometers
    """
    R = 6371  # Earth's radius in kilometers
    
    lat1 = radians(point1['lat'])
    lon1 = radians(point1['lon'])
    lat2 = radians(point2['lat'])
    lon2 = radians(point2['lon'])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    return R * c

def filter_events_by_distance(
    events: List[Event],
    user_location: Dict[str, float],
    max_distance: float
) -> List[Event]:
    """
    Filter events by distance from user location.
    
    Args:
        events: List of events to filter
        user_location: Dictionary with user's lat/lon coordinates
        max_distance: Maximum distance in kilometers
        
    Returns:
        List of events within max_distance
    """
    filtered_events = []
    
    for event in events:
        if not event.location or not event.location.coordinates:
            continue
            
        distance = calculate_distance(user_location, event.location.coordinates)
        if distance <= max_distance:
            filtered_events.append(event)
            
    return filtered_events

def sort_events_by_distance(
    events: List[Event],
    user_location: Dict[str, float]
) -> List[Event]:
    """
    Sort events by distance from user location.
    
    Args:
        events: List of events to sort
        user_location: Dictionary with user's lat/lon coordinates
        
    Returns:
        List of events sorted by distance (closest first)
    """
    def get_distance(event: Event) -> float:
        if not event.location or not event.location.coordinates:
            return float('inf')
        return calculate_distance(user_location, event.location.coordinates)
    
    return sorted(events, key=get_distance) 