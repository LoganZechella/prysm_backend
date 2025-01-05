from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from app.utils.schema import Event, Location, PriceInfo
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Validates event data quality and completeness"""
    
    REQUIRED_FIELDS = {
        'event_id': str,
        'title': str,
        'description': str,
        'start_datetime': datetime,
        'location': Location,
        'categories': list,
        'price_info': PriceInfo
    }
    
    def validate_event(self, event: Event) -> Tuple[bool, List[str]]:
        """
        Validate an event object against quality criteria
        Returns: (is_valid, list of validation errors)
        """
        errors = []
        
        # Check required fields
        for field, expected_type in self.REQUIRED_FIELDS.items():
            if not hasattr(event, field):
                errors.append(f"Missing required field: {field}")
                continue
                
            value = getattr(event, field)
            if not isinstance(value, expected_type):
                errors.append(f"Invalid type for {field}: expected {expected_type}, got {type(value)}")
        
        # Validate location data
        if hasattr(event, 'location'):
            if not all(hasattr(event.location, k) for k in ['venue_name', 'city', 'coordinates']):
                errors.append("Location missing required subfields")
            elif not isinstance(event.location.coordinates, dict):
                errors.append("Invalid coordinates format")
            elif not all(k in event.location.coordinates for k in ['lat', 'lng']):
                errors.append("Coordinates missing lat/lng")
        
        # Validate price info
        if hasattr(event, 'price_info'):
            if not all(hasattr(event.price_info, k) for k in ['min_price', 'max_price', 'price_tier']):
                errors.append("Price info missing required fields")
            elif not event.price_info.price_tier in ['free', 'budget', 'medium', 'premium']:
                errors.append("Invalid price tier")
            elif event.price_info.min_price > event.price_info.max_price:
                errors.append("Min price greater than max price")
        
        # Validate categories
        if hasattr(event, 'categories'):
            if not event.categories:
                errors.append("Event must have at least one category")
            elif not all(isinstance(c, str) for c in event.categories):
                errors.append("Categories must be strings")
        
        # Validate dates
        if hasattr(event, 'start_datetime'):
            try:
                if isinstance(event.start_datetime, str):
                    # Handle ISO format string with timezone
                    datetime.fromisoformat(event.start_datetime)
            except ValueError:
                errors.append("Invalid start_datetime format")
        
        return len(errors) == 0, errors
    
    def validate_events(self, events: List[Event]) -> Dict[str, Any]:
        """
        Validate a list of events and return statistics
        """
        stats = {
            'total_events': len(events),
            'valid_events': 0,
            'invalid_events': 0,
            'error_types': {},
            'invalid_event_ids': []
        }
        
        for event in events:
            is_valid, errors = self.validate_event(event)
            if is_valid:
                stats['valid_events'] += 1
            else:
                stats['invalid_events'] += 1
                stats['invalid_event_ids'].append(event.event_id)
                for error in errors:
                    stats['error_types'][error] = stats['error_types'].get(error, 0) + 1
        
        return stats 