from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta
from app.schemas.event import Event, PriceInfo

def validate_event(event: Event) -> bool:
    """
    Validate an event for data quality.
    
    Args:
        event: Event to validate
        
    Returns:
        True if event is valid, False otherwise
    """
    # Check required fields
    if not check_required_fields(event):
        return False
        
    # Check coordinates
    if not validate_coordinates(event.location.coordinates):
        return False
        
    # Check dates
    if not validate_dates(event.start_datetime, event.end_datetime):
        return False
        
    # Check prices if present
    if event.price_info and not validate_prices(event.price_info):
        return False
        
    return True

def check_required_fields(event: Event) -> bool:
    """
    Check that all required fields are present and valid.
    
    Args:
        event: Event to check
        
    Returns:
        True if all required fields are valid, False otherwise
    """
    # Check title
    if not event.title or len(event.title.strip()) == 0:
        return False
        
    # Check description
    if not event.description or len(event.description.strip()) == 0:
        return False
        
    # Check location
    if not event.location.venue_name or len(event.location.venue_name.strip()) == 0:
        return False
        
    if not event.location.city or len(event.location.city.strip()) == 0:
        return False
        
    return True

def validate_coordinates(coordinates: Dict[str, float]) -> bool:
    """
    Validate geographical coordinates.
    
    Args:
        coordinates: Dictionary with lat/lon coordinates
        
    Returns:
        True if coordinates are valid, False otherwise
    """
    if "lat" not in coordinates or "lon" not in coordinates:
        return False
        
    lat = coordinates["lat"]
    lon = coordinates["lon"]
    
    # Check latitude range (-90 to 90)
    if lat < -90 or lat > 90:
        return False
        
    # Check longitude range (-180 to 180)
    if lon < -180 or lon > 180:
        return False
        
    return True

def validate_dates(
    start_datetime: datetime,
    end_datetime: Optional[datetime]
) -> bool:
    """
    Validate event dates.
    
    Args:
        start_datetime: Event start datetime
        end_datetime: Optional event end datetime
        
    Returns:
        True if dates are valid, False otherwise
    """
    # End datetime is optional
    if end_datetime is None:
        return True
        
    # End must be after start
    return end_datetime > start_datetime

def validate_prices(price_info: PriceInfo) -> bool:
    """
    Validate price information.
    
    Args:
        price_info: Price information to validate
        
    Returns:
        True if prices are valid, False otherwise
    """
    # Check for negative prices
    if price_info.min_price < 0 or price_info.max_price < 0:
        return False
        
    # Check min <= max
    if price_info.min_price > price_info.max_price:
        return False
        
    # Check currency
    valid_currencies = {"USD", "EUR", "GBP"}
    if price_info.currency not in valid_currencies:
        return False
        
    return True 