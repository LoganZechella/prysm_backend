from typing import Dict, Any, Optional, Tuple
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import logging
from geopy.location import Location

logger = logging.getLogger(__name__)

class LocationService:
    def __init__(self):
        self.geocoder = Nominatim(user_agent="prysm_backend")
        
    def calculate_distance(self, location1: Dict[str, Any], location2: Dict[str, Any]) -> float:
        """
        Calculate the distance between two locations in kilometers.
        
        Args:
            location1: Dict with lat/lng or address
            location2: Dict with lat/lng or address
            
        Returns:
            Distance in kilometers
        """
        try:
            # Get coordinates for location1
            if 'lat' in location1 and 'lng' in location1:
                coords1 = (location1['lat'], location1['lng'])
            else:
                coords1 = self._get_coordinates(location1.get('address', ''))
                
            # Get coordinates for location2
            if 'lat' in location2 and 'lng' in location2:
                coords2 = (location2['lat'], location2['lng'])
            else:
                coords2 = self._get_coordinates(location2.get('address', ''))
                
            if not coords1 or not coords2:
                return float('inf')
                
            return geodesic(coords1, coords2).kilometers
            
        except Exception as e:
            logger.error(f"Error calculating distance: {str(e)}")
            return float('inf')
            
    def _get_coordinates(self, address: str) -> Optional[Tuple[float, float]]:
        """Get coordinates for an address using geocoding."""
        try:
            if not address:
                return None
                
            location: Optional[Location] = self.geocoder.geocode(address)
            if location:
                return (location.latitude, location.longitude)
            return None
            
        except Exception as e:
            logger.error(f"Error geocoding address: {str(e)}")
            return None
            
    def get_location_info(self, location: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get detailed location information including city, state, country.
        
        Args:
            location: Dict with lat/lng or address
            
        Returns:
            Dict with location details
        """
        try:
            # If we have coordinates, do reverse geocoding
            if 'lat' in location and 'lng' in location:
                result: Optional[Location] = self.geocoder.reverse((location['lat'], location['lng']))
            # Otherwise do forward geocoding
            else:
                result: Optional[Location] = self.geocoder.geocode(location.get('address', ''))
                
            if not result:
                return {}
                
            # Parse the address components
            address = result.raw.get('address', {})
            return {
                'city': address.get('city', ''),
                'state': address.get('state', ''),
                'country': address.get('country', ''),
                'postal_code': address.get('postcode', ''),
                'formatted_address': result.address,
                'lat': result.latitude,
                'lng': result.longitude
            }
            
        except Exception as e:
            logger.error(f"Error getting location info: {str(e)}")
            return {} 