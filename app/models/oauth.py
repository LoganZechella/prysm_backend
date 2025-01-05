from sqlalchemy import Column, String, DateTime, JSON
from sqlalchemy.sql import func
from app.database import Base

class OAuthToken(Base):
    """SQLAlchemy model for OAuth tokens"""
    __tablename__ = "oauth_tokens"

    user_id = Column(String, primary_key=True, index=True)
    service = Column(String, primary_key=True)  # e.g., 'spotify', 'google', 'linkedin'
    access_token = Column(String, nullable=False)
    refresh_token = Column(String)
    token_type = Column(String)
    scope = Column(String)
    expires_at = Column(DateTime(timezone=True))
    extra = Column(JSON, default=lambda: {}, server_default='{}')
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __init__(self, **kwargs):
        super().__init__()
        self.user_id = kwargs.get('user_id')
        self.service = kwargs.get('service')
        self.access_token = kwargs.get('access_token')
        self.refresh_token = kwargs.get('refresh_token')
        self.token_type = kwargs.get('token_type')
        self.scope = kwargs.get('scope')
        self.expires_at = kwargs.get('expires_at')
        self.extra = kwargs.get('extra', {}) 