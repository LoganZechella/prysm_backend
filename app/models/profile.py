from typing import List, Optional
from pydantic import BaseModel, Field

class LocationPreference(BaseModel):
    city: str
    state: Optional[str] = None
    country: str
    max_distance_km: float = Field(default=50.0, description="Maximum distance in kilometers")

class EventPreference(BaseModel):
    categories: List[str] = Field(default_factory=list, description="Preferred event categories")
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    preferred_days: List[str] = Field(
        default_factory=lambda: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    )
    preferred_times: List[str] = Field(
        default_factory=lambda: ["morning", "afternoon", "evening"]
    )

class UserProfile(BaseModel):
    user_id: str
    name: str
    email: str
    location_preference: LocationPreference
    event_preferences: EventPreference
    interests: List[str] = Field(default_factory=list, description="General interests and hobbies")
    excluded_categories: List[str] = Field(default_factory=list, description="Categories to exclude")
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user123",
                "name": "John Doe",
                "email": "john@example.com",
                "location_preference": {
                    "city": "San Francisco",
                    "state": "CA",
                    "country": "USA",
                    "max_distance_km": 50.0
                },
                "event_preferences": {
                    "categories": ["Music", "Tech", "Food & Drink"],
                    "min_price": 0,
                    "max_price": 100,
                    "preferred_days": ["Friday", "Saturday", "Sunday"],
                    "preferred_times": ["evening"]
                },
                "interests": ["Live Music", "Technology", "Startups", "Food"],
                "excluded_categories": ["Sports"]
            }
        } 