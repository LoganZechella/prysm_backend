import logging
from typing import Dict, Optional
import math

logger = logging.getLogger(__name__)

def geocode_address(
    address: str,
    city: str,
    state: str = "",
    country: str = "United States"
) -> Optional[Dict[str, float]]:
    """
    Geocode an address to get coordinates.
    Returns dict with 'lat' and 'lng' or None if geocoding fails.
    """
    # TODO: Implement actual geocoding using a service like Google Maps
    # For now, return None to indicate no coordinates available
    return None

def calculate_distance(coords1: Dict[str, float], coords2: Dict[str, float]) -> float:
    """
    Calculate distance between two coordinates using Haversine formula.
    Returns distance in kilometers.
    """
    R = 6371  # Earth's radius in kilometers
    
    lat1 = math.radians(coords1['lat'])
    lon1 = math.radians(coords1['lng'])
    lat2 = math.radians(coords2['lat'])
    lon2 = math.radians(coords2['lng'])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c 