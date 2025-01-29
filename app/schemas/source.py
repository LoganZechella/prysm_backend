"""Source information schema."""

from datetime import datetime
from pydantic import BaseModel, Field

class SourceInfo(BaseModel):
    """Information about the source of an event."""
    platform: str = Field(description="Platform where the event was found (e.g., 'eventbrite', 'meetup')")
    url: str = Field(description="Original URL of the event")
    last_updated: datetime = Field(description="When the event was last updated") 