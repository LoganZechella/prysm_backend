from sqlalchemy import Column, Integer, String, DateTime, JSON, Float, Boolean, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from app.database.base import Base

class EventModel(Base):
    """Event model for storing scraped events"""
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    platform_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)  # Original ID from the source platform
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String)
    start_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_datetime: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    url: Mapped[str] = mapped_column(String, nullable=False)
    
    # Venue information
    venue_name: Mapped[str] = mapped_column(String)
    venue_lat: Mapped[float] = mapped_column(Float)
    venue_lon: Mapped[float] = mapped_column(Float)
    venue_city: Mapped[str] = mapped_column(String)
    venue_state: Mapped[str] = mapped_column(String)
    venue_country: Mapped[str] = mapped_column(String)
    
    # Organizer information
    organizer_id: Mapped[str] = mapped_column(String)
    organizer_name: Mapped[str] = mapped_column(String)
    
    # Event metadata
    platform: Mapped[str] = mapped_column(String, nullable=False)  # 'meetup', 'eventbrite', 'facebook'
    is_online: Mapped[bool] = mapped_column(Boolean, default=False)
    rsvp_count: Mapped[int] = mapped_column(Integer, default=0)
    price_info: Mapped[dict] = mapped_column(JSONB, nullable=True)  # {currency: str, min_price: float, max_price: float}
    categories: Mapped[list] = mapped_column(ARRAY(String), nullable=True)
    image_url: Mapped[str] = mapped_column(String, nullable=True)
    
    # ML-related fields
    technical_level: Mapped[float] = mapped_column(Float, default=0.5)  # 0.0 to 1.0, representing event complexity
    
    # System fields
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    last_scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    scores = relationship("EventScore", back_populates="event")
    feedback = relationship("UserFeedback", back_populates="event")
    implicit_feedback = relationship("ImplicitFeedback", back_populates="event")

    def to_dict(self) -> dict:
        """Convert event to dictionary."""
        return {
            'id': self.id,
            'platform_id': self.platform_id,
            'title': self.title,
            'description': self.description,
            'start_datetime': self.start_datetime.isoformat() if self.start_datetime else None,
            'end_datetime': self.end_datetime.isoformat() if self.end_datetime else None,
            'url': self.url,
            'venue': {
                'name': self.venue_name,
                'latitude': self.venue_lat,
                'longitude': self.venue_lon,
                'city': self.venue_city,
                'state': self.venue_state,
                'country': self.venue_country
            },
            'organizer': {
                'id': self.organizer_id,
                'name': self.organizer_name
            },
            'platform': self.platform,
            'is_online': self.is_online,
            'rsvp_count': self.rsvp_count,
            'price_info': self.price_info,
            'categories': self.categories,
            'image_url': self.image_url,
            'technical_level': self.technical_level,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'last_scraped_at': self.last_scraped_at.isoformat() if self.last_scraped_at else None
        }

    class Config:
        orm_mode = True 