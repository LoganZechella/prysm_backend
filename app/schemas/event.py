"""
Event schema definitions.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class Location(BaseModel):
    """Location schema."""
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

class PriceInfo(BaseModel):
    """Price information schema."""
    currency: str
    min_price: Optional[float] = None
    max_price: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

class EventBase(BaseModel):
    """Base schema for events."""
    platform_id: str
    title: str
    description: str
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    url: str
    
    # Venue information
    venue_name: Optional[str] = None
    venue_lat: Optional[float] = None
    venue_lon: Optional[float] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_country: Optional[str] = None
    
    # Organizer information
    organizer_id: Optional[str] = None
    organizer_name: Optional[str] = None
    
    # Event metadata
    platform: str
    is_online: bool = False
    rsvp_count: int = 0
    price_info: Optional[Dict[str, Any]] = None
    categories: Optional[List[str]] = None
    image_url: Optional[str] = None
    
    # ML-related fields
    technical_level: float = 0.5

    model_config = ConfigDict(from_attributes=True)

class EventCreate(EventBase):
    """Schema for creating events."""
    pass

class EventInDB(EventBase):
    """Schema for events with database fields."""
    id: int
    created_at: datetime
    updated_at: datetime
    last_scraped_at: datetime

    model_config = ConfigDict(from_attributes=True)

class EventResponse(EventBase):
    """Schema for event responses."""
    id: int
    created_at: datetime
    updated_at: datetime
    last_scraped_at: datetime

    model_config = ConfigDict(from_attributes=True)

class EventUpdate(BaseModel):
    """Schema for updating events."""
    title: Optional[str] = None
    description: Optional[str] = None
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    url: Optional[str] = None
    venue_name: Optional[str] = None
    venue_lat: Optional[float] = None
    venue_lon: Optional[float] = None
    venue_city: Optional[str] = None
    venue_state: Optional[str] = None
    venue_country: Optional[str] = None
    organizer_id: Optional[str] = None
    organizer_name: Optional[str] = None
    is_online: Optional[bool] = None
    rsvp_count: Optional[int] = None
    price_info: Optional[Dict[str, Any]] = None
    categories: Optional[List[str]] = None
    image_url: Optional[str] = None
    technical_level: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

class Event(EventBase):
    """Schema for events."""
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_scraped_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)

class SourceInfo(BaseModel):
    """Source information schema."""
    platform: str
    url: str
    last_updated: datetime

    model_config = ConfigDict(from_attributes=True)

class EventAttributes(BaseModel):
    """Event attributes schema."""
    indoor_outdoor: str
    age_restriction: str

    model_config = ConfigDict(from_attributes=True)

class EventMetadata(BaseModel):
    """Event metadata schema."""
    popularity_score: float

    model_config = ConfigDict(from_attributes=True)

class ImageAnalysis(BaseModel):
    """Image analysis results schema."""
    labels: List[str] = []
    objects: List[str] = []
    text: List[str] = []
    safe_search: Dict[str, str] = {}
    width: Optional[int] = None
    height: Optional[int] = None
    format: Optional[str] = None
    url: Optional[str] = None

    model_config = ConfigDict(from_attributes=True) 