from sqlalchemy import Column, Integer, String, DateTime, JSON, Float, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from app.database import Base

class Event(Base):
    """Event model for storing scraped events"""
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    location: Mapped[dict] = mapped_column(JSONB)  # {lat: float, lng: float}
    categories: Mapped[list] = mapped_column(ARRAY(String))  # List of strings
    price_info: Mapped[dict] = mapped_column(JSONB)  # {currency: str, min_price: float, max_price: float, price_tier: str}
    source: Mapped[str] = mapped_column(String, nullable=False)  # Platform name (e.g., 'eventbrite', 'meetup', etc.)
    source_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)  # Original ID from the source platform
    url: Mapped[str] = mapped_column(String, nullable=False)  # Event URL
    image_url: Mapped[str] = mapped_column(String)  # Main event image URL
    venue: Mapped[dict] = mapped_column(JSONB)  # {name: str, address: str}
    organizer: Mapped[dict] = mapped_column(JSONB)  # {name: str, description: str}
    tags: Mapped[list] = mapped_column(ARRAY(String))  # List of strings
    view_count: Mapped[int] = mapped_column(Integer, default=0)
    like_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def to_dict(self) -> dict:
        """Convert event to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "location": self.location,
            "categories": self.categories,
            "price_info": self.price_info,
            "source": self.source,
            "source_id": self.source_id,
            "url": self.url,
            "image_url": self.image_url,
            "venue": self.venue,
            "organizer": self.organizer,
            "tags": self.tags,
            "view_count": self.view_count,
            "like_count": self.like_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None
        }

    class Config:
        orm_mode = True 