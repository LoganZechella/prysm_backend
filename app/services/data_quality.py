"""Data quality validation for events."""

from typing import Dict, Optional, List
from datetime import datetime
from app.schemas.event import EventBase, PriceInfo

def validate_event(event: EventBase) -> bool:
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
        
    # Check coordinates if present
    if event.venue_lat is not None and event.venue_lon is not None:
        if not validate_coordinates({
            "lat": event.venue_lat,
            "lon": event.venue_lon
        }):
            return False
        
    # Check dates
    if not validate_dates(event.start_datetime, event.end_datetime):
        return False
        
    # Check prices if present
    if event.price_info:
        price_info = PriceInfo(
            currency=event.price_info["currency"],
            min_price=event.price_info["min_price"],
            max_price=event.price_info["max_price"]
        )
        if not validate_prices(price_info):
            return False
        
    return True

def check_required_fields(event: EventBase) -> bool:
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
        
    # Check start datetime
    if not event.start_datetime:
        return False
        
    # Check venue
    if not event.venue_name or not event.venue_city:
        return False
        
    # Check platform and url
    if not event.platform or not event.url:
        return False
        
    # Check categories
    if not event.categories:
        return False
        
    return True

def validate_coordinates(coordinates: Dict[str, float]) -> bool:
    """
    Validate geographical coordinates.
    
    Args:
        coordinates: Dictionary containing lat/lon coordinates
        
    Returns:
        True if coordinates are valid, False otherwise
    """
    # Check required keys
    if 'lat' not in coordinates or 'lon' not in coordinates:
        return False
        
    lat = coordinates['lat']
    lon = coordinates['lon']
    
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
    if price_info.min_price is not None and price_info.min_price < 0:
        return False
        
    if price_info.max_price is not None and price_info.max_price < 0:
        return False
        
    # Check min <= max if both are present
    if price_info.min_price is not None and price_info.max_price is not None:
        if price_info.min_price > price_info.max_price:
            return False
        
    # Check currency
    valid_currencies = {"USD", "EUR", "GBP"}
    if price_info.currency not in valid_currencies:
        return False
        
    return True 