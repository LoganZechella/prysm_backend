from sqlalchemy import Column, String, DateTime, Float, JSON
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base

class EventModel(Base):
    """SQLAlchemy model for events."""
    
    __tablename__ = "events"
    
    event_id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=False)
    start_datetime = Column(DateTime, nullable=False)
    end_datetime = Column(DateTime, nullable=True)
    location = Column(JSONB, nullable=False)  # Stores Location as JSON
    categories = Column(JSONB, nullable=False)  # List of strings
    attributes = Column(JSONB, nullable=False)  # Stores EventAttributes as JSON
    source = Column(JSONB, nullable=False)  # Stores SourceInfo as JSON
    metadata = Column(JSONB, nullable=False)  # Stores EventMetadata as JSON
    price_info = Column(JSONB, nullable=True)  # Stores PriceInfo as JSON 