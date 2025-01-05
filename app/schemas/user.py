from typing import Dict, List, Optional, Tuple
from pydantic import BaseModel, Field, constr
from datetime import time

PriceTier = constr(pattern=r"^(free|low|medium|high|premium)$")
DayOfWeek = constr(pattern=r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$")
IndoorOutdoor = constr(pattern=r"^(indoor|outdoor|both)$")

class UserPreferences(BaseModel):
    user_id: str
    preferred_categories: List[str] = []
    price_preferences: List[PriceTier] = []
    preferred_location: Optional[Dict[str, float]] = None  # {"lat": float, "lng": float}
    max_distance: float = Field(ge=0, default=50.0)  # in miles
    preferred_times: List[Tuple[time, time]] = []  # List of (start_time, end_time) tuples
    min_price: float = Field(ge=0, default=0.0)
    max_price: float = Field(ge=0, default=float('inf'))
    preferred_days: List[DayOfWeek] = []
    accessibility_requirements: List[str] = []
    indoor_outdoor_preference: Optional[IndoorOutdoor] = None
    excluded_categories: List[str] = []
    min_rating: float = Field(ge=0, le=1, default=0.0)
    saved_events: List[str] = []  # List of event IDs
    favorite_venues: List[str] = []  # List of venue names
    notification_preferences: Dict[str, bool] = {
        "email": True,
        "push": True,
        "sms": False
    } 