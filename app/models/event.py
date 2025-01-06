from sqlalchemy import Column, String, DateTime, Float, JSON, Integer
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from app.database import Base
from datetime import datetime
from typing import Dict, Any, List, Optional

class Event(Base):
    """SQLAlchemy model for events."""
    
    __tablename__ = "events"
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(String, nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    location = Column(JSONB, nullable=False)  # Dict with lat/lng
    categories = Column(ARRAY(String), nullable=False)
    price_info = Column(JSONB, nullable=True)  # Dict with price details
    source = Column(String, nullable=False)  # Source platform (e.g., "eventbrite")
    source_id = Column(String, nullable=False, unique=True)  # Original ID from source
    url = Column(String, nullable=True)  # Link to original event
    image_url = Column(String, nullable=True)
    venue = Column(JSONB, nullable=True)  # Venue details
    organizer = Column(JSONB, nullable=True)  # Organizer details
    tags = Column(ARRAY(String), nullable=True)
    view_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary."""
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'start_time': self.start_time.isoformat() if getattr(self, 'start_time', None) else None,
            'end_time': self.end_time.isoformat() if getattr(self, 'end_time', None) else None,
            'location': self.location,
            'categories': self.categories,
            'price_info': self.price_info,
            'source': self.source,
            'source_id': self.source_id,
            'url': self.url,
            'image_url': self.image_url,
            'venue': self.venue,
            'organizer': self.organizer,
            'tags': self.tags,
            'view_count': self.view_count,
            'like_count': self.like_count,
            'created_at': self.created_at.isoformat() if getattr(self, 'created_at', None) else None,
            'updated_at': self.updated_at.isoformat() if getattr(self, 'updated_at', None) else None
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create an Event instance from a dictionary."""
        # Convert ISO format strings to datetime objects
        for field in ['start_time', 'end_time', 'created_at', 'updated_at']:
            if isinstance(data.get(field), str):
                data[field] = datetime.fromisoformat(data[field])
                
        return cls(**data)
        
    def __init__(self, **kwargs):
        """Initialize an Event instance."""
        for key, value in kwargs.items():
            setattr(self, key, value) 