from sqlalchemy import Column, String, Float, JSON, DateTime
from sqlalchemy.sql import func
from app.database import Base

class UserPreferences(Base):
    """SQLAlchemy model for user preferences"""
    __tablename__ = "user_preferences"

    user_id = Column(String, primary_key=True, index=True)
    preferred_categories = Column(JSON, default=lambda: [], server_default='[]')
    excluded_categories = Column(JSON, default=lambda: [], server_default='[]')
    min_price = Column(Float, default=0.0, server_default='0.0')
    max_price = Column(Float, default=1000.0, server_default='1000.0')
    preferred_location = Column(JSON, default=lambda: {}, server_default='{}')
    preferred_days = Column(JSON, default=lambda: [], server_default='[]')
    preferred_times = Column(JSON, default=lambda: [], server_default='[]')
    min_rating = Column(Float, default=0.5, server_default='0.5')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __init__(self, **kwargs):
        super().__init__()
        self.user_id = kwargs.get('user_id')
        self.preferred_categories = kwargs.get('preferred_categories', [])
        self.excluded_categories = kwargs.get('excluded_categories', [])
        self.min_price = kwargs.get('min_price', 0.0)
        self.max_price = kwargs.get('max_price', 1000.0)
        self.preferred_location = kwargs.get('preferred_location', {})
        self.preferred_days = kwargs.get('preferred_days', [])
        self.preferred_times = kwargs.get('preferred_times', [])
        self.min_rating = kwargs.get('min_rating', 0.5)

    def to_dict(self):
        """Convert model to dictionary"""
        return {
            "user_id": self.user_id,
            "preferred_categories": self.preferred_categories or [],
            "excluded_categories": self.excluded_categories or [],
            "min_price": self.min_price,
            "max_price": self.max_price,
            "preferred_location": self.preferred_location or {},
            "preferred_days": self.preferred_days or [],
            "preferred_times": self.preferred_times or [],
            "min_rating": self.min_rating,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        } 