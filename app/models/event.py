from sqlalchemy import Column, Integer, String, DateTime, JSON, Float
from sqlalchemy.sql import func
from app.db.session import Base

class Event(Base):
    """Event model for storing scraped events"""
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    description = Column(String)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    location = Column(JSON)  # {lat: float, lng: float}
    categories = Column(JSON)  # List of strings
    price_info = Column(JSON)  # {currency: str, min_price: float, max_price: float, price_tier: str}
    source = Column(String, nullable=False)  # Platform name (e.g., 'eventbrite', 'meetup', etc.)
    source_id = Column(String, nullable=False)  # Original ID from the source platform
    url = Column(String, nullable=False)  # Event URL
    image_url = Column(String)  # Main event image URL
    venue = Column(JSON)  # {name: str, address: str}
    organizer = Column(JSON)  # {name: str, description: str}
    tags = Column(JSON)  # List of strings
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    popularity_score = Column(Float, default=0.5)  # Score between 0 and 1

    class Config:
        orm_mode = True 