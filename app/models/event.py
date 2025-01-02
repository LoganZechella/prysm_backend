from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime

class Location(BaseModel):
    """Location information for an event"""
    venue_name: str
    address: str
    city: str
    state: str
    country: str
    coordinates: Dict[str, float]  # {"lat": float, "lng": float}

class PriceInfo(BaseModel):
    """Price information for an event"""
    currency: str = "USD"
    min_price: float = 0
    max_price: float = 0
    price_tier: str = "free"  # One of: free, budget, medium, premium

class SourceInfo(BaseModel):
    """Source information for an event"""
    platform: str  # e.g., "meta", "eventbrite"
    url: str
    last_updated: datetime

class Event(BaseModel):
    """Standard event model"""
    event_id: str
    title: str
    description: str
    short_description: str
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    location: Location
    categories: List[str]
    tags: List[str]
    price_info: PriceInfo
    images: List[str]
    source: SourceInfo 