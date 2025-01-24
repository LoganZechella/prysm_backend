"""
Event schema definitions.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class EventBase(BaseModel):
    """Base schema for events."""
    user_id: str
    provider: str
    event_type: str
    event_data: Dict[str, Any]
    timestamp: datetime

    model_config = ConfigDict(from_attributes=True)

class EventCreate(EventBase):
    """Schema for creating events."""
    pass

class Event(EventBase):
    """Schema for events with database fields."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class EventResponse(EventBase):
    """Schema for event responses."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True) 