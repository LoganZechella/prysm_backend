from sqlalchemy import Column, String, Float, JSON, Integer
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from app.database import Base

class UserPreferencesModel(Base):
    """SQLAlchemy model for user preferences."""
    
    __tablename__ = "user_preferences"
    
    user_id = Column(String, primary_key=True)
    preferred_categories = Column(ARRAY(String), nullable=False, default=[])
    excluded_categories = Column(ARRAY(String), nullable=False, default=[])
    min_price = Column(Float, nullable=True)
    max_price = Column(Float, nullable=True)
    preferred_location = Column(JSONB, nullable=True)  # Stores location coordinates
    preferred_days = Column(ARRAY(Integer), nullable=False, default=[])  # 0-6 for days of week
    preferred_times = Column(ARRAY(Integer), nullable=False, default=[])  # 0-23 for hours
    min_rating = Column(Float, nullable=True)
    max_distance = Column(Float, nullable=True)  # Distance in kilometers 