"""Authentication service implementation."""

from typing import Dict, Optional
from datetime import datetime, timedelta
import os
from sqlalchemy.orm import Session
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.oauth import OAuthToken
from app.auth import create_access_token
import logging
from app.utils.spotify import SpotifyClient

logger = logging.getLogger(__name__)

async def get_auth_status(user_id: str, db: AsyncSession) -> Dict[str, bool]:
    """
    Get authentication status for all providers for a user.
    
    Args:
        user_id: User ID to check
        db: Database session
        
    Returns:
        Dictionary of provider statuses
    """
    result = await db.execute(
        select(OAuthToken).filter_by(user_id=user_id)
    )
    tokens = result.scalars().all()
    
    # Initialize all providers as False
    status = {
        "spotify": False,
        "google": False,
        "linkedin": False
    }
    
    # Update status for providers with valid tokens
    for token in tokens:
        if token.expires_at and token.expires_at > datetime.utcnow():
            status[token.provider] = True
        elif token.refresh_token:
            # Try to refresh expired token
            if await refresh_spotify_token(user_id, db):
                status[token.provider] = True
            
    return status

def get_spotify_auth_url() -> str:
    """
    Get Spotify OAuth authorization URL.
    
    Returns:
        Authorization URL string
    """
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
    scope = "user-read-private user-read-email"
    
    if not client_id or not redirect_uri:
        raise ValueError("Missing Spotify OAuth configuration")
        
    return f"https://accounts.spotify.com/authorize?client_id={client_id}&response_type=code&redirect_uri={redirect_uri}&scope={scope}"

async def process_spotify_callback(
    code: Optional[str],
    state: Optional[str],
    error: Optional[str],
    db: AsyncSession
) -> str:
    """
    Process Spotify OAuth callback.
    
    Args:
        code: Authorization code
        state: State parameter containing user ID
        error: Error message
        db: Database session
        
    Returns:
        Redirect URL
    """
    if error:
        logger.error(f"Spotify OAuth error: {error}")
        return f"/auth/error?error={error}"
        
    if not code or not state:
        logger.error("Missing code or state in Spotify callback")
        return "/auth/error?error=invalid_request"
        
    try:
        spotify = SpotifyClient()
        token_info = await spotify.exchange_code(code)
        
        # Save token to database
        token = OAuthToken(
            user_id=state,
            provider="spotify",
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
            redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
            access_token=token_info["access_token"],
            refresh_token=token_info["refresh_token"],
            expires_at=datetime.utcnow() + timedelta(seconds=token_info["expires_in"])
        )
        
        # Check for existing token
        result = await db.execute(
            select(OAuthToken).filter_by(
                user_id=state,
                provider="spotify"
            )
        )
        existing_token = result.scalar_one_or_none()
        
        if existing_token:
            # Update existing token
            existing_token.access_token = token.access_token
            existing_token.refresh_token = token.refresh_token
            existing_token.expires_at = token.expires_at
        else:
            # Add new token
            db.add(token)
            
        await db.commit()
        return "/auth/success"
        
    except Exception as e:
        logger.error(f"Error processing Spotify callback: {str(e)}")
        return f"/auth/error?error={str(e)}"

async def refresh_spotify_token(user_id: str, db: AsyncSession) -> bool:
    """
    Refresh Spotify OAuth token.
    
    Args:
        user_id: User ID
        db: Database session
        
    Returns:
        True if token was refreshed successfully
    """
    try:
        # Get existing token
        result = await db.execute(
            select(OAuthToken).filter_by(
                user_id=user_id,
                provider="spotify"
            )
        )
        token = result.scalar_one_or_none()
        
        if not token or not token.refresh_token:
            logger.error(f"No refresh token found for user {user_id}")
            return False
            
        # Refresh token
        spotify = SpotifyClient()
        new_token_info = await spotify.refresh_token(token.refresh_token)
        
        # Update token in database using update statement
        await db.execute(
            update(OAuthToken)
            .where(
                OAuthToken.user_id == user_id,
                OAuthToken.provider == "spotify"
            )
            .values(
                access_token=new_token_info["access_token"],
                refresh_token=new_token_info.get("refresh_token", token.refresh_token),
                expires_at=datetime.utcnow() + timedelta(seconds=new_token_info["expires_in"]),
                updated_at=datetime.utcnow()
            )
        )
        
        await db.commit()
        return True
        
    except Exception as e:
        logger.error(f"Error refreshing Spotify token: {str(e)}")
        await db.rollback()
        return False 