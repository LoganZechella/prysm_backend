"""Test configuration and fixtures."""

import os
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
from app.database import Base
from app.services.event_collection import EventCollectionService
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.auth import create_access_token
from app.models.oauth import OAuthToken

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://logan@localhost:5432/test_db"

# Test Scrapfly API key
TEST_SCRAPFLY_API_KEY = "test_key"

# Test JWT secret key
os.environ["JWT_SECRET_KEY"] = "test_secret_key"
os.environ["JWT_ALGORITHM"] = "HS256"
os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = "30"

# Configure test database
engine = create_async_engine(TEST_DATABASE_URL)
TestingSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Create tables
async def init_test_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

# Dependency override
async def override_get_db():
    async with TestingSessionLocal() as session:
        yield session

@pytest.fixture
def test_user_id():
    """Get test user ID."""
    return "test-user-123"

@pytest.fixture
def test_access_token(test_user_id):
    """Create test JWT access token."""
    return create_access_token(test_user_id)

@pytest.fixture
def auth_headers(test_access_token):
    """Get auth headers with test token."""
    return {"Authorization": f"Bearer {test_access_token}"}

@pytest.fixture
async def test_db():
    """Create test database session."""
    async with TestingSessionLocal() as session:
        yield session
        # Cleanup
        await session.rollback()
        await session.close()

# Event collection service fixture
@pytest.fixture
def event_collection_service():
    """Create event collection service for testing."""
    return EventCollectionService(scrapfly_api_key=TEST_SCRAPFLY_API_KEY)

@pytest.fixture
def mock_oauth_token(test_db, test_user_id):
    """Create mock OAuth token."""
    token = OAuthToken(
        user_id=test_user_id,
        provider="spotify",
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri="http://localhost:8000/callback",
        access_token="test_access_token",
        refresh_token="test_refresh_token",
        expires_at=datetime.utcnow() + timedelta(hours=1)
    )
    test_db.add(token)
    test_db.commit()
    return token 