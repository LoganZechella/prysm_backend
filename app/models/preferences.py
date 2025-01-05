from sqlalchemy import Column, String, Float, DateTime, TypeDecorator, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from app.database import Base
from sqlalchemy.orm import object_session
from datetime import datetime

class UserPreferences(Base):
    """SQLAlchemy model for user preferences"""
    __tablename__ = "user_preferences"

    user_id = Column(String, primary_key=True, index=True)
    preferred_categories = Column(JSONB, default=lambda: [], server_default='[]')
    excluded_categories = Column(JSONB, default=lambda: [], server_default='[]')
    min_price = Column(Float, default=0.0, server_default='0.0')
    max_price = Column(Float, default=1000.0, server_default='1000.0')
    preferred_location = Column(JSONB, default=lambda: {}, server_default='{}')
    preferred_days = Column(JSONB, default=lambda: [], server_default='[]')
    preferred_times = Column(JSONB, default=lambda: [], server_default='[]')
    min_rating = Column(Float, default=0.5, server_default='0.5')
    accessibility_requirements = Column(JSONB, default=lambda: [], server_default='[]')
    indoor_outdoor_preference = Column(String(50), nullable=True)
    age_restriction_preference = Column(String(50), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __init__(self, **kwargs):
        super().__init__()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self):
        """Convert model to dictionary"""
        session = object_session(self)
        if session:
            session.refresh(self)
            
        created_at = getattr(self, 'created_at', None)
        updated_at = getattr(self, 'updated_at', None)
        
        return {
            "user_id": getattr(self, 'user_id', None),
            "preferred_categories": getattr(self, 'preferred_categories', []),
            "excluded_categories": getattr(self, 'excluded_categories', []),
            "min_price": float(getattr(self, 'min_price', 0.0)),
            "max_price": float(getattr(self, 'max_price', 1000.0)),
            "preferred_location": getattr(self, 'preferred_location', {}),
            "preferred_days": getattr(self, 'preferred_days', []),
            "preferred_times": getattr(self, 'preferred_times', []),
            "min_rating": float(getattr(self, 'min_rating', 0.5)),
            "accessibility_requirements": getattr(self, 'accessibility_requirements', []),
            "indoor_outdoor_preference": getattr(self, 'indoor_outdoor_preference', None),
            "age_restriction_preference": getattr(self, 'age_restriction_preference', None),
            "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            "updated_at": updated_at.isoformat() if isinstance(updated_at, datetime) else None
        } 