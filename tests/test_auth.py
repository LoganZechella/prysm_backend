import pytest
from fastapi.testclient import TestClient
from app.models.oauth import OAuthToken
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import json

def test_auth_status_no_tokens(client, mock_supertokens):
    """Test auth status endpoint with no OAuth tokens."""
    response = client.get("/api/auth/status")
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "spotify": False,
        "google": False,
        "linkedin": False
    }

def test_auth_status_with_tokens(client, test_db, mock_supertokens):
    """Test auth status endpoint with OAuth tokens."""
    # Add test tokens
    tokens = [
        OAuthToken(
            user_id="test-user-123",
            service="spotify",
            access_token="test_token",
            expires_at=datetime.utcnow() + timedelta(hours=1)
        ),
        OAuthToken(
            user_id="test-user-123",
            service="google",
            access_token="test_token",
            expires_at=datetime.utcnow() + timedelta(hours=1)
        )
    ]
    test_db.add_all(tokens)
    test_db.commit()
    
    response = client.get("/api/auth/status")
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "spotify": True,
        "google": True,
        "linkedin": False
    }

def test_spotify_auth_url(client):
    """Test Spotify auth URL generation."""
    with patch.dict("os.environ", {"SPOTIFY_CLIENT_ID": "test_client_id"}):
        response = client.get("/api/auth/spotify")
        assert response.status_code == 200
        data = response.json()
        assert "auth_url" in data
        assert "client_id=test_client_id" in data["auth_url"]
        assert "response_type=code" in data["auth_url"]

def test_spotify_callback_success(client, test_db, mock_supertokens):
    """Test successful Spotify OAuth callback."""
    # Mock Spotify OAuth response
    token_info = {
        "access_token": "test_access_token",
        "refresh_token": "test_refresh_token",
        "token_type": "Bearer",
        "scope": "user-read-private",
        "expires_in": 3600
    }
    
    with patch("spotipy.oauth2.SpotifyOAuth") as mock_oauth:
        mock_oauth_instance = MagicMock()
        mock_oauth_instance.get_access_token.return_value = token_info
        mock_oauth.return_value = mock_oauth_instance
        
        response = client.get("/api/auth/spotify/callback?code=test_code")
        assert response.status_code == 303  # Redirect
        assert "success=true" in response.headers["location"]
        
        # Verify token was stored
        stored_token = test_db.query(OAuthToken).filter_by(
            user_id="test-user-123",
            service="spotify"
        ).first()
        
        assert stored_token is not None
        assert stored_token.access_token == token_info["access_token"]
        assert stored_token.refresh_token == token_info["refresh_token"]

def test_spotify_callback_error(client):
    """Test Spotify OAuth callback with error."""
    response = client.get("/api/auth/spotify/callback?error=access_denied")
    assert response.status_code == 303  # Redirect
    assert "error=access_denied" in response.headers["location"]

def test_token_refresh(client, test_db, mock_supertokens):
    """Test OAuth token refresh."""
    # Create expired token
    expired_token = OAuthToken(
        user_id="test-user-123",
        service="spotify",
        access_token="old_access_token",
        refresh_token="test_refresh_token",
        token_type="Bearer",
        scope="user-read-private",
        expires_at=datetime.utcnow() - timedelta(hours=1)
    )
    test_db.add(expired_token)
    test_db.commit()
    
    # Mock new token response
    new_token = {
        "access_token": "new_access_token",
        "refresh_token": "new_refresh_token",
        "token_type": "Bearer",
        "scope": "user-read-private",
        "expires_in": 3600
    }
    
    with patch("spotipy.oauth2.SpotifyOAuth") as mock_oauth:
        mock_oauth_instance = MagicMock()
        mock_oauth_instance.refresh_access_token.return_value = new_token
        mock_oauth.return_value = mock_oauth_instance
        
        # Get auth status (should trigger token refresh)
        response = client.get("/api/auth/status")
        assert response.status_code == 200
        data = response.json()
        assert data["spotify"] is True
        
        # Verify token was updated
        updated_token = test_db.query(OAuthToken).filter_by(
            user_id="test-user-123",
            service="spotify"
        ).first()
        
        assert updated_token.access_token == new_token["access_token"]
        assert updated_token.refresh_token == new_token["refresh_token"]

def test_token_refresh_failure(client, test_db, mock_supertokens):
    """Test OAuth token refresh failure."""
    # Create expired token
    expired_token = OAuthToken(
        user_id="test-user-123",
        service="spotify",
        access_token="old_access_token",
        refresh_token="invalid_refresh_token",
        token_type="Bearer",
        scope="user-read-private",
        expires_at=datetime.utcnow() - timedelta(hours=1)
    )
    test_db.add(expired_token)
    test_db.commit()
    
    with patch("spotipy.oauth2.SpotifyOAuth") as mock_oauth:
        mock_oauth_instance = MagicMock()
        mock_oauth_instance.refresh_access_token.side_effect = Exception("Token refresh failed")
        mock_oauth.return_value = mock_oauth_instance
        
        # Get auth status (should handle refresh failure gracefully)
        response = client.get("/api/auth/status")
        assert response.status_code == 200
        data = response.json()
        assert data["spotify"] is False  # Service should be marked as disconnected

def test_session_management(client, mock_supertokens):
    """Test basic session management."""
    # Test session initialization
    response = client.get("/api/auth/init-session")
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    
    # Test session already exists
    response = client.get("/api/auth/init-session")
    assert response.status_code == 200
    assert response.json()["message"] == "Session already exists" 