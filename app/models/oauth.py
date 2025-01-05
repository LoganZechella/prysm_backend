from sqlalchemy import Column, String, DateTime, JSON, Integer
from sqlalchemy.sql import func
from app.database import Base

class OAuthToken(Base):
    """SQLAlchemy model for OAuth tokens"""
    __tablename__ = "oauth_tokens"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), nullable=False)
    provider = Column(String(50), nullable=False)  # e.g., 'spotify', 'google', 'linkedin'
    access_token = Column(String(2000), nullable=False)
    refresh_token = Column(String(2000))
    expires_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __init__(self, **kwargs):
        super().__init__()
        self.user_id = kwargs.get('user_id')
        self.provider = kwargs.get('provider')
        self.access_token = kwargs.get('access_token')
        self.refresh_token = kwargs.get('refresh_token')
        self.expires_at = kwargs.get('expires_at') 