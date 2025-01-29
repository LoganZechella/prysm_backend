"""Database models for the application."""

from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from app.models.event import EventModel
from app.database.base import Base

__all__ = ['Base', 'User', 'UserProfile', 'EventScore', 'UserFeedback', 'ImplicitFeedback', 'EventModel']

class User(Base):
    """User model for authentication and profile data."""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    profile = relationship("UserProfile", back_populates="user", uselist=False)
    feedback = relationship("UserFeedback", back_populates="user")
    implicit_feedback = relationship("ImplicitFeedback", back_populates="user")

class UserProfile(Base):
    """Extended user profile information."""
    __tablename__ = "user_profiles"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    preferences = Column(JSON)
    interests = Column(JSON)
    location = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="profile")

class EventScore(Base):
    """Stores computed scores for events."""
    __tablename__ = "event_scores"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, ForeignKey("events.id"))
    score_type = Column(String)  # relevance, popularity, etc.
    score = Column(Float)
    score_metadata = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    event = relationship("EventModel", back_populates="scores")

class UserFeedback(Base):
    """Explicit user feedback on events."""
    __tablename__ = "user_feedback"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    event_id = Column(Integer, ForeignKey("events.id"))
    rating = Column(Float)
    feedback_type = Column(String)  # rating, like, etc.
    comment = Column(String, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="feedback")
    event = relationship("EventModel", back_populates="feedback")

class ImplicitFeedback(Base):
    """Implicit user feedback (clicks, views, etc.)."""
    __tablename__ = "implicit_feedback"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    event_id = Column(Integer, ForeignKey("events.id"))
    interaction_type = Column(String)  # click, view, etc.
    interaction_data = Column(JSON)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="implicit_feedback")
    event = relationship("EventModel", back_populates="implicit_feedback") 