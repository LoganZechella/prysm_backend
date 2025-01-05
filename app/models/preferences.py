from sqlalchemy import Column, String, Float, JSON, DateTime, func
from app.database import Base
from datetime import datetime

class UserPreferences(Base):
    __tablename__ = "user_preferences"
    
    user_id = Column(String, primary_key=True)
    preferred_categories = Column(JSON, default=list)
    excluded_categories = Column(JSON, default=list)
    min_price = Column(Float, default=0.0)
    max_price = Column(Float, default=float('inf'))
    preferred_location = Column(JSON, default=dict)
    preferred_days = Column(JSON, default=list)
    preferred_times = Column(JSON, default=list)
    min_rating = Column(Float, default=0.5)
    accessibility_requirements = Column(JSON, default=list)
    indoor_outdoor_preference = Column(String, nullable=True)
    age_restriction_preference = Column(String, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
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
            'accessibility_requirements': getattr(self, 'accessibility_requirements', []),
            'indoor_outdoor_preference': getattr(self, 'indoor_outdoor_preference', None),
            'age_restriction_preference': getattr(self, 'age_restriction_preference', None),
            'created_at': created_at.isoformat() if isinstance(created_at, datetime) else None,
            'updated_at': updated_at.isoformat() if isinstance(updated_at, datetime) else None
        } 