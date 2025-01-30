"""Event score model for storing event scoring data."""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base

class EventScore(Base):
    """Model for storing event scores."""
    __tablename__ = "event_scores"
    __table_args__ = {'extend_existing': True}

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"))
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    
    # Score components
    category_score: Mapped[float] = mapped_column(Float, default=0.0)
    price_score: Mapped[float] = mapped_column(Float, default=0.0)
    location_score: Mapped[float] = mapped_column(Float, default=0.0)
    time_score: Mapped[float] = mapped_column(Float, default=0.0)
    engagement_score: Mapped[float] = mapped_column(Float, default=0.0)
    final_score: Mapped[float] = mapped_column(Float, default=0.0)
    
    # Score metadata
    score_context: Mapped[dict] = mapped_column(JSON, nullable=True)  # Additional scoring context
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    event = relationship("EventModel", back_populates="scores")
    
    def to_dict(self) -> dict:
        """Convert score to dictionary."""
        return {
            'id': self.id,
            'event_id': self.event_id,
            'user_id': self.user_id,
            'scores': {
                'category_score': self.category_score,
                'price_score': self.price_score,
                'location_score': self.location_score,
                'time_score': self.time_score,
                'engagement_score': self.engagement_score,
                'final_score': self.final_score
            },
            'score_context': self.score_context,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        } 