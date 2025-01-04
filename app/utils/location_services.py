import os
import logging
from typing import Dict, Optional, Tuple
import googlemaps
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

# Initialize Google Maps client
gmaps = googlemaps.Client(key=os.getenv('GOOGLE_MAPS_API_KEY', ''))

def geocode_address(address: str, city: str = "", state: str = "", country: str = "") -> Optional[Dict[str, float]]:
    """
    Convert an address to coordinates using Google Maps Geocoding API.
    Returns a dict with 'lat' and 'lng' keys, or None if geocoding fails.
    """
    try:
        # Construct full address
        full_address_parts = [p for p in [address, city, state, country] if p]
        full_address = ", ".join(full_address_parts)
        
        # Get geocoding result
        result = gmaps.geocode(full_address) # type: ignore
        
        if result and len(result) > 0:
            location = result[0]['geometry']['location']
            return {
                'lat': location['lat'],
                'lng': location['lng']
            }
        return None
        
    except Exception as e:
        logger.error(f"Geocoding error for address '{full_address}': {str(e)}")
        return None

def calculate_distance(coord1: Dict[str, float], coord2: Dict[str, float]) -> float:
    """
    Calculate distance between two coordinates in kilometers using Google Maps Distance Matrix API.
    """
    try:
        origin = f"{coord1['lat']},{coord1['lng']}"
        dest = f"{coord2['lat']},{coord2['lng']}"
        
        result = gmaps.distance_matrix( # type: ignore
            origins=[origin],
            destinations=[dest],
            mode="driving",
            units="metric"
        )
        
        if result['rows'][0]['elements'][0]['status'] == 'OK':
            # Return distance in kilometers
            return result['rows'][0]['elements'][0]['distance']['value'] / 1000
        return 0.0
        
    except Exception as e:
        logger.error(f"Distance calculation error: {str(e)}")
        return 0.0 