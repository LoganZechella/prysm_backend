"""Event metadata schema."""

from typing import Optional
from pydantic import BaseModel, Field

class EventMetadata(BaseModel):
    """Additional metadata about an event."""
    popularity_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Popularity score of the event (0.0 to 1.0)"
    )
    engagement_rate: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="User engagement rate with the event (0.0 to 1.0)"
    )
    quality_score: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Quality score of the event content (0.0 to 1.0)"
    ) 