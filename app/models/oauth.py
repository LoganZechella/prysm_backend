from sqlalchemy import Column, String, DateTime, JSON, Integer
from sqlalchemy.sql import func
from app.database import Base

class OAuthToken(Base):
    """SQLAlchemy model for OAuth tokens"""
    __tablename__ = "oauth_tokens"

    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), nullable=False)
    provider = Column(String(50), nullable=False)  # e.g., 'spotify', 'google', 'linkedin'
    
    # OAuth credentials
    client_id = Column(String(255), nullable=False)
    client_secret = Column(String(255), nullable=False)
    redirect_uri = Column(String(255), nullable=False)
    
    # Token data
    access_token = Column(String(2000), nullable=False)
    refresh_token = Column(String(2000))
    token_type = Column(String(50), default='Bearer')
    scope = Column(String(1000))
    expires_at = Column(DateTime(timezone=True))
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    last_used_at = Column(DateTime(timezone=True))
    
    # Additional provider-specific data
    provider_user_id = Column(String(255))  # ID from the provider (e.g., Spotify user ID)
    provider_metadata = Column(JSON)  # Additional provider-specific data

    def __init__(self, **kwargs):
        """Initialize an OAuth token."""
        super().__init__()
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        
        # Set defaults for optional fields
        if not self.token_type:
            self.token_type = 'Bearer'
        if not self.provider_metadata:
            self.provider_metadata = {} 