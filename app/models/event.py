from sqlalchemy import Column, Integer, String, DateTime, JSON, Float
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
    start_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime)
    location: Mapped[dict] = mapped_column(JSON)  # {lat: float, lng: float}
    categories: Mapped[list] = mapped_column(JSON)  # List of strings
    price_info: Mapped[dict] = mapped_column(JSON)  # {currency: str, min_price: float, max_price: float, price_tier: str}
    source: Mapped[str] = mapped_column(String, nullable=False)  # Platform name (e.g., 'eventbrite', 'meetup', etc.)
    source_id: Mapped[str] = mapped_column(String, nullable=False)  # Original ID from the source platform
    url: Mapped[str] = mapped_column(String, nullable=False)  # Event URL
    image_url: Mapped[str] = mapped_column(String)  # Main event image URL
    venue: Mapped[dict] = mapped_column(JSON)  # {name: str, address: str}
    organizer: Mapped[dict] = mapped_column(JSON)  # {name: str, description: str}
    tags: Mapped[list] = mapped_column(JSON)  # List of strings
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    popularity_score: Mapped[float] = mapped_column(Float, default=0.5)  # Score between 0 and 1

    class Config:
        orm_mode = True 