import pytest
from fastapi.testclient import TestClient
from app.models.oauth import OAuthToken
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import json
from app.auth import create_access_token, get_current_user
from fastapi import HTTPException, Request
from sqlalchemy import select, delete
from app.services.auth_service import get_auth_status, get_spotify_auth_url, process_spotify_callback, refresh_spotify_token
from app.utils.spotify import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from app.database.base import Base

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def engine():
    """Create a test database engine."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture
async def db_session(engine):
    """Create a test database session."""
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()

@pytest.fixture
def test_user_id():
    """Test user ID fixture."""
    return "test-user-123"

@pytest.fixture
def auth_headers(test_user_id):
    """Auth headers fixture with JWT token."""
    token = create_access_token(test_user_id)
    return {"Authorization": f"Bearer {token}"}

@pytest.fixture
async def mock_oauth_token(db_session, test_user_id):
    """Create a mock OAuth token."""
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
    db_session.add(token)
    await db_session.commit()
    return token

@pytest.fixture(autouse=True)
async def cleanup_tokens(db_session):
    """Clean up any existing tokens before each test."""
    await db_session.execute(delete(OAuthToken))
    await db_session.commit()

@pytest.mark.asyncio
async def test_auth_status_no_tokens(db_session, test_user_id, auth_headers):
    """Test auth status endpoint with no OAuth tokens."""
    status = await get_auth_status(test_user_id, db_session)
    assert status == {
        "spotify": False,
        "google": False,
        "linkedin": False
    }

@pytest.mark.asyncio
async def test_auth_status_with_tokens(db_session, test_user_id, auth_headers, mock_oauth_token):
    """Test auth status endpoint with OAuth tokens."""
    # Add Google token
    google_token = OAuthToken(
        user_id=test_user_id,
        provider="google",
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri="http://localhost:8000/callback",
        access_token="test_access_token",
        refresh_token="test_refresh_token",
        expires_at=datetime.utcnow() + timedelta(hours=1)
    )
    db_session.add(google_token)
    await db_session.commit()

    # Check auth status
    status = await get_auth_status(test_user_id, db_session)
    assert status["google"] == True
    assert status["spotify"] == True
    assert status["linkedin"] == False

@pytest.mark.asyncio
async def test_spotify_auth_url():
    """Test Spotify auth URL generation."""
    with patch.dict("os.environ", {
        "SPOTIFY_CLIENT_ID": "test_client_id",
        "SPOTIFY_REDIRECT_URI": "http://localhost:8000/callback"
    }):
        url = get_spotify_auth_url()
        assert "client_id=test_client_id" in url
        assert "redirect_uri=http://localhost:8000/callback" in url

@pytest.mark.asyncio
async def test_spotify_callback_success(db_session, test_user_id, auth_headers):
    """Test successful Spotify OAuth callback."""
    # Mock Spotify client response
    mock_token_info = {
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "token_type": "Bearer",
        "scope": "user-read-private",
        "expires_in": 3600
    }

    with patch.object(SpotifyClient, "exchange_code", return_value=mock_token_info):
        redirect_url = await process_spotify_callback(
            code="test_code",
            state=test_user_id,
            error=None,
            db=db_session
        )
        assert redirect_url == "/auth/success"

        # Verify token was saved
        result = await db_session.execute(
            select(OAuthToken).filter_by(user_id=test_user_id, provider="spotify")
        )
        token = result.scalar_one()
        assert token.access_token == "test_access_token"
        assert token.refresh_token == "test_refresh_token"

@pytest.mark.asyncio
async def test_spotify_callback_error(db_session):
    """Test Spotify OAuth callback with error."""
    redirect_url = await process_spotify_callback(
        code=None,
        state="test-user-123",
        error="access_denied",
        db=db_session
    )
    assert redirect_url == "/auth/error?error=access_denied"

@pytest.mark.asyncio
async def test_token_refresh(db_session, test_user_id, auth_headers):
    """Test OAuth token refresh."""
    # Create expired token
    expired_token = OAuthToken(
        user_id=test_user_id,
        provider="spotify",
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri="http://localhost:8000/callback",
        access_token="old_access_token",
        refresh_token="test_refresh_token",
        expires_at=datetime.utcnow() - timedelta(hours=1)
    )
    db_session.add(expired_token)
    await db_session.commit()

    # Mock new token response
    mock_token_info = {
        "access_token": "new_access_token",
        "refresh_token": "new_refresh_token",
        "token_type": "Bearer",
        "scope": "user-read-private",
        "expires_in": 3600
    }

    with patch.object(SpotifyClient, "refresh_token", return_value=mock_token_info):
        # Refresh token
        success = await refresh_spotify_token(test_user_id, db_session)
        assert success is True

        # Verify token was updated
        result = await db_session.execute(
            select(OAuthToken).filter_by(user_id=test_user_id, provider="spotify")
        )
        token = result.scalar_one()
        assert token.access_token == "new_access_token"
        assert token.refresh_token == "new_refresh_token"
        assert token.expires_at > datetime.utcnow()

@pytest.mark.asyncio
async def test_token_refresh_failure(db_session, test_user_id, auth_headers):
    """Test OAuth token refresh failure."""
    # Create expired token
    expired_token = OAuthToken(
        user_id=test_user_id,
        provider="spotify",
        client_id="test_client_id",
        client_secret="test_client_secret",
        redirect_uri="http://localhost:8000/callback",
        access_token="old_access_token",
        refresh_token="invalid_refresh_token",
        expires_at=datetime.utcnow() - timedelta(hours=1)
    )
    db_session.add(expired_token)
    await db_session.commit()

    with patch.object(SpotifyClient, "refresh_token", side_effect=Exception("Token refresh failed")):
        # Attempt to refresh token
        success = await refresh_spotify_token(test_user_id, db_session)
        assert success is False

        # Verify token was not updated
        result = await db_session.execute(
            select(OAuthToken).filter_by(user_id=test_user_id, provider="spotify")
        )
        token = result.scalar_one()
        assert token.access_token == "old_access_token"
        assert token.refresh_token == "invalid_refresh_token"

@pytest.mark.asyncio
async def test_get_current_user(auth_headers):
    """Test get_current_user dependency."""
    mock_request = MagicMock()
    mock_request.headers = auth_headers
    
    user_id = await get_current_user(mock_request)
    assert user_id == "test-user-123"

@pytest.mark.asyncio
async def test_get_current_user_no_token():
    """Test get_current_user with no token."""
    mock_request = MagicMock()
    mock_request.headers = {}
    
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(mock_request)
    assert exc_info.value.status_code == 401

@pytest.mark.asyncio
async def test_get_current_user_invalid_token():
    """Test get_current_user with invalid token."""
    mock_request = MagicMock()
    mock_request.headers = {"Authorization": "Bearer invalid_token"}
    
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(mock_request)
    assert exc_info.value.status_code == 401