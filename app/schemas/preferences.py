"""User preferences schema."""

from typing import List, Optional, Tuple
from datetime import time
from pydantic import BaseModel, Field

class UserPreferences(BaseModel):
    """User preferences for event recommendations."""
    user_id: str = Field(description="Unique identifier for the user")
    preferred_categories: List[str] = Field(
        default_factory=list,
        description="List of preferred event categories"
    )
    price_preferences: List[str] = Field(
        default_factory=list,
        description="List of acceptable price tiers ('free', 'low', 'medium', 'high')"
    )
    preferred_location: dict = Field(
        description="Dictionary containing lat/lng coordinates of preferred location"
    )
    max_distance: float = Field(
        default=50.0,
        ge=0.0,
        description="Maximum distance in kilometers from preferred location"
    )
    preferred_times: List[Tuple[time, time]] = Field(
        default_factory=list,
        description="List of preferred time ranges (start_time, end_time)"
    )
    min_price: Optional[float] = Field(
        default=0.0,
        ge=0.0,
        description="Minimum acceptable price"
    )
    max_price: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Maximum acceptable price"
    )
    preferred_days: List[str] = Field(
        default_factory=list,
        description="List of preferred days of the week"
    )
    accessibility_requirements: List[str] = Field(
        default_factory=list,
        description="List of required accessibility features"
    )
    indoor_outdoor_preference: Optional[str] = Field(
        default=None,
        description="Preference for indoor/outdoor events ('indoor', 'outdoor', None for no preference)"
    ) 