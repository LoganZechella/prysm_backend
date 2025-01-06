from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel

class EventBase(BaseModel):
    """Base Pydantic model for events."""
    title: str
    description: str
    start_time: datetime
    end_time: Optional[datetime] = None
    location: Dict[str, Any]  # Dict with lat/lng
    categories: List[str]
    price_info: Optional[Dict[str, Any]] = None  # Dict with price details
    source: str  # Source platform (e.g., "eventbrite")
    source_id: str  # Original ID from source
    url: Optional[str] = None  # Link to original event
    image_url: Optional[str] = None
    venue: Optional[Dict[str, Any]] = None  # Venue details
    organizer: Optional[Dict[str, Any]] = None  # Organizer details
    tags: Optional[List[str]] = None
    view_count: int = 0
    like_count: int = 0

class EventCreate(EventBase):
    """Pydantic model for creating events."""
    pass

class EventResponse(EventBase):
    """Pydantic model for event responses."""
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        """Pydantic model configuration."""
        from_attributes = True  # Allows conversion from SQLAlchemy models 