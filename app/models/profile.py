"""User profile models."""
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from app.models.preferences import LocationPreference, UserPreferencesBase

class Profile(BaseModel):
    """Schema for user profiles."""
    id: int
    user_id: str
    traits: Dict[str, Any]
    preferences: Optional[UserPreferencesBase] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "user_id": "user123",
                "traits": {
                    "music_traits": {
                        "genres": ["Rock", "Jazz", "Electronic"],
                        "artists": ["Artist1", "Artist2"],
                        "venues": ["Venue1", "Venue2"]
                    },
                    "professional_traits": {
                        "industries": ["Technology", "Music"],
                        "skills": ["Programming", "Music Production"],
                        "interests": ["Live Events", "Technology", "Music"]
                    }
                },
                "preferences": {
                    "user_id": "user123",
                    "preferred_categories": ["Music", "Tech", "Food & Drink"],
                    "excluded_categories": ["Sports"],
                    "min_price": 0.0,
                    "max_price": 100.0,
                    "preferred_location": {
                        "city": "San Francisco",
                        "state": "CA",
                        "country": "US",
                        "latitude": 37.7749,
                        "longitude": -122.4194,
                        "max_distance": 50.0
                    },
                    "preferred_days": [5, 6, 0],  # Friday, Saturday, Sunday
                    "preferred_times": [18, 19, 20, 21, 22]  # Evening hours
                }
            }
        }
    ) 