"""User preferences model."""
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import Column, String, Float, JSON, DateTime, func
from pydantic import BaseModel, Field, ConfigDict
from app.database import Base

class LocationPreference(BaseModel):
    """Location preference model."""
    city: str
    state: Optional[str] = None
    country: str = "US"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    max_distance: float = Field(default=50.0, description="Maximum distance in kilometers")

class UserPreferencesBase(BaseModel):
    """Base model for user preferences."""
    user_id: str
    preferred_categories: List[str] = Field(default_factory=list)
    excluded_categories: List[str] = Field(default_factory=list)
    min_price: float = Field(default=0.0)
    max_price: Optional[float] = None
    preferred_location: Optional[LocationPreference] = None
    preferred_days: List[int] = Field(default_factory=list)  # 0-6 for days of week
    preferred_times: List[int] = Field(default_factory=list)  # 0-23 for hours
    min_rating: float = Field(default=0.5)
    max_distance: Optional[float] = Field(default=50.0)  # Distance in kilometers

    model_config = ConfigDict(from_attributes=True)

class UserPreferences(Base):
    """SQLAlchemy model for user preferences."""
    __tablename__ = "user_preferences"

    user_id = Column(String, primary_key=True)
    preferred_categories = Column(JSON, default=list)
    excluded_categories = Column(JSON, default=list)
    min_price = Column(Float, default=0.0)
    max_price = Column(Float, nullable=True)
    preferred_location = Column(JSON, nullable=True)
    preferred_days = Column(JSON, default=list)
    preferred_times = Column(JSON, default=list)
    min_rating = Column(Float, default=0.5)
    max_distance = Column(Float, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def to_pydantic(self) -> UserPreferencesBase:
        """Convert SQLAlchemy model to Pydantic model."""
        data = {
            "user_id": self.user_id,
            "preferred_categories": self.preferred_categories or [],
            "excluded_categories": self.excluded_categories or [],
            "min_price": float(self.min_price or 0.0),
            "max_price": float(self.max_price) if self.max_price is not None else None,
            "preferred_location": LocationPreference(**self.preferred_location) if self.preferred_location else None,
            "preferred_days": self.preferred_days or [],
            "preferred_times": self.preferred_times or [],
            "min_rating": float(self.min_rating or 0.5),
            "max_distance": float(self.max_distance) if self.max_distance is not None else 50.0
        }
        return UserPreferencesBase(**data)

    @classmethod
    def from_pydantic(cls, preferences: UserPreferencesBase) -> "UserPreferences":
        """Create SQLAlchemy model from Pydantic model."""
        return cls(
            user_id=preferences.user_id,
            preferred_categories=preferences.preferred_categories,
            excluded_categories=preferences.excluded_categories,
            min_price=preferences.min_price,
            max_price=preferences.max_price,
            preferred_location=preferences.preferred_location.model_dump() if preferences.preferred_location else None,
            preferred_days=preferences.preferred_days,
            preferred_times=preferences.preferred_times,
            min_rating=preferences.min_rating,
            max_distance=preferences.max_distance
        )

    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        created_at = getattr(self, 'created_at', None)
        updated_at = getattr(self, 'updated_at', None)
        return {
            'user_id': str(getattr(self, 'user_id', '')),
            'preferred_categories': getattr(self, 'preferred_categories', []),
            'excluded_categories': getattr(self, 'excluded_categories', []),
            'min_price': float(getattr(self, 'min_price', 0.0)),
            'max_price': float(getattr(self, 'max_price', float('inf'))),
            'preferred_location': getattr(self, 'preferred_location', {}),
            'preferred_days': getattr(self, 'preferred_days', []),
            'preferred_times': getattr(self, 'preferred_times', []),
            'min_rating': float(getattr(self, 'min_rating', 0.5)),
            'max_distance': float(getattr(self, 'max_distance', 50.0)),
            'created_at': created_at.isoformat() if isinstance(created_at, datetime) else None,
            'updated_at': updated_at.isoformat() if isinstance(updated_at, datetime) else None
        } 