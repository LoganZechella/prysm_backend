"""Traits model for storing user traits extracted from various sources."""
from datetime import datetime
import uuid
from sqlalchemy import Column, String, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB, UUID

from app.database import Base


class Traits(Base):
    """Model for storing user traits."""
    __tablename__ = "traits"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(String, nullable=False, index=True)
    music_traits = Column(JSONB, nullable=False, server_default=text("'{}'"))
    social_traits = Column(JSONB, nullable=False, server_default=text("'{}'"))
    behavior_traits = Column(JSONB, nullable=False, server_default=text("'{}'"))
    professional_traits = Column(JSONB, nullable=False, server_default=text("'{}'"))
    trait_metadata = Column(JSONB, nullable=False, server_default=text('\'{"version": 1}\''))
    last_updated_at = Column(DateTime(timezone=True), nullable=False)
    next_update_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=text("now()"), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=text("now()"), onupdate=text("now()"), nullable=False)

    def __repr__(self):
        """String representation of the traits model."""
        return f"<Traits(user_id={self.user_id}, last_updated_at={self.last_updated_at})>" 