"""Feedback models for storing user feedback on events."""
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, JSON, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base

class UserFeedback(Base):
    """Model for storing explicit user feedback on events."""
    __tablename__ = "user_feedback"
    __table_args__ = {'extend_existing': True}

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"))
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    
    # Feedback data
    rating: Mapped[float] = mapped_column(Float, nullable=True)  # 0.0 to 5.0
    liked: Mapped[bool] = mapped_column(Boolean, nullable=True)
    saved: Mapped[bool] = mapped_column(Boolean, nullable=True)
    comment: Mapped[str] = mapped_column(String, nullable=True)
    
    # Feedback metadata
    feedback_context: Mapped[dict] = mapped_column(JSON, nullable=True)  # Additional feedback context
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_synthetic: Mapped[bool] = mapped_column(Boolean, default=False)  # Flag for synthetic data
    
    # Relationships
    event = relationship("EventModel", back_populates="feedback")
    
    def to_dict(self) -> dict:
        """Convert feedback to dictionary."""
        return {
            'id': self.id,
            'event_id': self.event_id,
            'user_id': self.user_id,
            'rating': self.rating,
            'liked': self.liked,
            'saved': self.saved,
            'comment': self.comment,
            'feedback_context': self.feedback_context,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }

class ImplicitFeedback(Base):
    """Model for storing implicit user feedback on events."""
    __tablename__ = "implicit_feedback"
    __table_args__ = {'extend_existing': True}

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"))
    user_id: Mapped[str] = mapped_column(String, nullable=False)
    
    # Feedback data
    view_count: Mapped[int] = mapped_column(Integer, default=0)
    view_duration: Mapped[float] = mapped_column(Float, default=0.0)  # Total view time in seconds
    click_count: Mapped[int] = mapped_column(Integer, default=0)
    share_count: Mapped[int] = mapped_column(Integer, default=0)
    
    # Feedback metadata
    feedback_context: Mapped[dict] = mapped_column(JSON, nullable=True)  # Additional feedback context
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_synthetic: Mapped[bool] = mapped_column(Boolean, default=False)  # Flag for synthetic data
    
    # Relationships
    event = relationship("EventModel", back_populates="implicit_feedback")
    
    def to_dict(self) -> dict:
        """Convert feedback to dictionary."""
        return {
            'id': self.id,
            'event_id': self.event_id,
            'user_id': self.user_id,
            'view_count': self.view_count,
            'view_duration': self.view_duration,
            'click_count': self.click_count,
            'share_count': self.share_count,
            'feedback_context': self.feedback_context,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        } 