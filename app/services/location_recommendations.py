from typing import Dict, Any, Optional, Tuple, List, Union
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import logging
from geopy.location import Location
from math import radians, sin, cos, sqrt, atan2
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
import requests
import time
from functools import lru_cache

logger = logging.getLogger(__name__)

# Constants
DEFAULT_COUNTRY = "US"
DEFAULT_MAX_DISTANCE = 50.0  # kilometers
USER_AGENT = "prysm_backend"
GEOCODER_TIMEOUT = 5  # seconds
MAX_RETRIES = 3

class LocationService:
    """Service for location-based operations and recommendations."""
    
    def __init__(self):
        """Initialize the location service."""
        self.geolocator = Nominatim(
            user_agent=USER_AGENT,
            timeout=GEOCODER_TIMEOUT
        )
        
    def _geocode_venue_uncached(self, venue_key: str, address: Dict[str, str]) -> Optional[Tuple[float, float]]:
        """
        Internal method to geocode a venue address without caching.
        """
        try:
            # Validate inputs
            if not isinstance(venue_key, str) or not isinstance(address, dict):
                logger.error(f"Invalid input types: venue_key={type(venue_key)}, address={type(address)}")
                return None

            if not venue_key.strip():
                logger.warning("Empty venue key provided")
                return None

            if not address.get('city') or not address.get('country'):
                logger.warning(f"Incomplete address for venue: {venue_key}, missing city or country")
                return None
            
            # Build address string from components
            address_parts = [
                venue_key.split('|')[0],  # venue name is first part
                address.get('city', ''),
                address.get('state', ''),
                address.get('country', '')
            ]
            address_str = ', '.join(part.strip() for part in address_parts if part.strip())
            
            # Try geocoding with exponential backoff
            for attempt in range(MAX_RETRIES):
                try:
                    location = self.geolocator.geocode(address_str)
                    if location and hasattr(location, 'latitude') and hasattr(location, 'longitude'):
                        return (location.latitude, location.longitude)
                    logger.warning(f"No location found for address: {address_str}")
                    break
                except (GeocoderTimedOut, GeocoderUnavailable) as e:
                    if attempt == MAX_RETRIES - 1:  # Last attempt
                        logger.error(f"Error geocoding venue {venue_key}: {str(e)}")
                        return None
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    time.sleep(wait_time)
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error geocoding venue {venue_key}: {str(e)}")
                    return None
                    
            return None
            
        except Exception as e:
            logger.error(f"Error geocoding venue {venue_key}: {str(e)}")
            return None

    @lru_cache(maxsize=1000)
    def geocode_venue(self, venue_key: str, address_str: str) -> Optional[Tuple[float, float]]:
        """
        Cached wrapper for geocoding a venue address.
        
        Args:
            venue_key: A unique string key identifying the venue (e.g. "venue_name|city|state|country")
            address_str: JSON string representation of the address dictionary
            
        Returns:
            Tuple with latitude and longitude if found
        """
        try:
            import json
            address = json.loads(address_str)
            return self._geocode_venue_uncached(venue_key, address)
        except Exception as e:
            logger.error(f"Error in cached geocode_venue: {str(e)}")
            return None
            
    def calculate_distance(
        self,
        point1: Dict[str, float],
        point2: Dict[str, float]
    ) -> float:
        """
        Calculate distance between two points in kilometers.
        
        Args:
            point1: Dictionary with lat/lon or lat/lng coordinates
            point2: Dictionary with lat/lon or lat/lng coordinates
            
        Returns:
            Distance in kilometers
        """
        try:
            # Handle both lon and lng keys
            lon1 = point1.get("lon") or point1.get("lng")
            lon2 = point2.get("lon") or point2.get("lng")
            
            if not all([point1.get("lat"), lon1, point2.get("lat"), lon2]):
                logger.error("Missing coordinates for distance calculation")
                return float("inf")
                
            coords1 = (point1["lat"], lon1)
            coords2 = (point2["lat"], lon2)
            return geodesic(coords1, coords2).kilometers
            
        except Exception as e:
            logger.error(f"Error calculating distance: {str(e)}")
            return float("inf")  # Return infinity on error
            
    def _get_event_coordinates(self, event: Union[Dict[str, Any], Any]) -> Optional[Dict[str, float]]:
        """
        Extract coordinates from an event object.
        
        Args:
            event: Event object (dictionary or model)
            
        Returns:
            Dictionary with lat/lon coordinates if found
        """
        try:
            if isinstance(event, dict):
                # Handle dictionary events
                venue = event.get("venue", {})
                return {
                    "lat": venue.get("lat") or event.get("venue_lat"),
                    "lon": venue.get("lon") or venue.get("lng") or event.get("venue_lon")
                }
            else:
                # Handle model events
                return {
                    "lat": getattr(event, "venue_lat", None),
                    "lon": getattr(event, "venue_lon", None)
                }
        except Exception as e:
            logger.error(f"Error extracting coordinates: {str(e)}")
            return None
            
    def filter_by_distance(
        self,
        events: List[Union[Dict[str, Any], Any]],
        user_location: Dict[str, float],
        max_distance_km: float = DEFAULT_MAX_DISTANCE
    ) -> List[Union[Dict[str, Any], Any]]:
        """
        Filter events by distance from user location.
        
        Args:
            events: List of events with venue coordinates
            user_location: User's location coordinates (lat/lon or lat/lng)
            max_distance_km: Maximum distance in kilometers
            
        Returns:
            Filtered list of events
        """
        filtered_events = []
        
        for event in events:
            venue_coords = self._get_event_coordinates(event)
            if not venue_coords or not all(venue_coords.values()):
                continue
                
            distance = self.calculate_distance(user_location, venue_coords)
            if distance <= max_distance_km:
                if isinstance(event, dict):
                    event["distance_km"] = round(distance, 2)
                else:
                    setattr(event, "distance_km", round(distance, 2))
                filtered_events.append(event)
                
        return filtered_events
        
    def sort_by_distance(
        self,
        events: List[Union[Dict[str, Any], Any]],
        user_location: Dict[str, float]
    ) -> List[Union[Dict[str, Any], Any]]:
        """
        Sort events by distance from user location.
        
        Args:
            events: List of events with venue coordinates
            user_location: User's location coordinates (lat/lon or lat/lng)
            
        Returns:
            Sorted list of events
        """
        def get_distance(event: Union[Dict[str, Any], Any]) -> float:
            venue_coords = self._get_event_coordinates(event)
            if not venue_coords or not all(venue_coords.values()):
                return float("inf")
                
            return self.calculate_distance(user_location, venue_coords)
            
        return sorted(events, key=get_distance)
        
    def get_coordinates(self, location: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """
        Get coordinates from location dictionary.
        
        Args:
            location: Dictionary with lat/lon, lat/lng, or address
            
        Returns:
            Dictionary with lat, lon, and lng if found
        """
        try:
            # If coordinates provided directly
            if all(k in location for k in ["lat", "lon"]) or all(k in location for k in ["lat", "lng"]):
                lon = location.get("lon") or location.get("lng")
                return {
                    "lat": float(location["lat"]),
                    "lon": float(lon),
                    "lng": float(lon)
                }
            
            # If address provided, geocode it
            if "address" in location:
                location_data = self.geolocator.geocode(location["address"])
                if location_data:
                    return {
                        "lat": location_data.latitude,
                        "lon": location_data.longitude,
                        "lng": location_data.longitude
                    }
                logger.warning(f"Could not geocode address: {location['address']}")
                return None
                
            logger.error("Invalid location format. Must provide lat/lon, lat/lng, or address")
            return None
            
        except Exception as e:
            logger.error(f"Error getting coordinates: {str(e)}")
            return None

# Remove duplicate module-level functions since they're now handled by the class 