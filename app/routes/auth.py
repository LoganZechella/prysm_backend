from fastapi import APIRouter, HTTPException, Request, Depends
from typing import Optional
from pydantic import BaseModel
from app.utils.auth import verify_session, create_session
from app.models.oauth import OAuthToken
from app.database import get_db
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import logging
import os
from urllib.parse import urlencode
import spotipy
from fastapi.responses import RedirectResponse
from supertokens_python.recipe.session.asyncio import revoke_session
import httpx
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter()

class AuthStatus(BaseModel):
    spotify: bool = False
    google: bool = False
    linkedin: bool = False

@router.get("/status")
async def auth_status_handler(
    request: Request,
    db: Session = Depends(get_db)
):
    """Get the current authentication status."""
    try:
        # Try to get session
        try:
            session = await verify_session(request)
            user_id = session.get_user_id()
            
            # Check each service's connection status
            spotify_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                service="spotify"
            ).first()
            
            google_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                service="google"
            ).first()
            
            linkedin_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                service="linkedin"
            ).first()
            
            # Return connection status for each service
            return AuthStatus(
                spotify=bool(spotify_token),
                google=bool(google_token),
                linkedin=bool(linkedin_token)
            )
        except:
            # No active session
            return AuthStatus()
            
    except Exception as e:
        logger.error(f"Error checking auth status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to check auth status"
        )

@router.get("/init-session")
async def init_session_handler(request: Request):
    """Initialize a session for the user if one doesn't exist."""
    try:
        # Try to get existing session
        try:
            session = await verify_session(request)
            return {"status": "success", "message": "Session already exists"}
        except:
            # Create a new session with a temporary user ID
            temp_user_id = f"temp_{datetime.utcnow().timestamp()}"
            await create_session(request, temp_user_id)
            return {"status": "success", "message": "Session created"}
            
    except Exception as e:
        logger.error(f"Error initializing session: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to initialize session"
        )

@router.get("/spotify")
async def spotify_auth_handler():
    """Handle Spotify OAuth authorization."""
    try:
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/auth/spotify/callback")
        scope = "user-read-private user-read-email playlist-read-private playlist-read-collaborative user-top-read"
        
        if not client_id:
            raise HTTPException(
                status_code=500,
                detail="Spotify client ID not configured"
            )
        
        # Construct Spotify authorization URL
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scope,
            "show_dialog": True
        }
        
        auth_url = f"https://accounts.spotify.com/authorize?{urlencode(params)}"
        return {"auth_url": auth_url}
        
    except Exception as e:
        logger.error(f"Error initiating Spotify auth: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to initiate Spotify authorization"
        )

@router.get("/spotify/callback")
async def spotify_callback_handler(
    request: Request,
    code: str, 
    error: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Handle Spotify OAuth callback."""
    if error:
        logger.error(f"Spotify authorization error: {error}")
        return RedirectResponse(
            url=f"http://localhost:3001/auth/dashboard?error={error}",
            status_code=303
        )
        
    try:
        # Exchange the authorization code for access token
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/auth/spotify/callback")
        
        if not client_id or not client_secret:
            logger.error("Spotify credentials not configured")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=configuration_error",
                status_code=303
            )
            
        # Create Spotify OAuth client
        auth = spotipy.oauth2.SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope="user-read-private user-read-email playlist-read-private playlist-read-collaborative user-top-read"
        )
        
        # Exchange code for token
        token_info = auth.get_access_token(code)
        
        if not token_info:
            logger.error("Failed to exchange code for token")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=token_exchange_failed",
                status_code=303
            )
        
        try:
            # Try to get session
            session = await verify_session(request)
            user_id = session.get_user_id()
            
            # Store token in database
            oauth_token = OAuthToken(
                user_id=user_id,
                service="spotify",
                access_token=token_info["access_token"],
                refresh_token=token_info.get("refresh_token"),
                token_type=token_info.get("token_type", "Bearer"),
                scope=token_info.get("scope", ""),
                expires_at=datetime.utcnow() + timedelta(seconds=token_info.get("expires_in", 3600))
            )
            
            # Update or insert token
            existing_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                service="spotify"
            ).first()
            
            if existing_token:
                for key, value in oauth_token.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing_token, key, value)
            else:
                db.add(oauth_token)
                
            db.commit()
            
            # Update session with Spotify connection status
            await session.merge_into_access_token_payload({
                "spotify_connected": True
            })
            
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?success=true",
                status_code=303
            )
            
        except Exception as e:
            logger.error(f"Error storing token: {str(e)}")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=token_storage_failed",
                status_code=303
            )
        
    except Exception as e:
        logger.error(f"Error handling Spotify callback: {str(e)}", exc_info=True)
        return RedirectResponse(
            url="http://localhost:3001/auth/dashboard?error=authorization_failed",
            status_code=303
        ) 

@router.post("/logout")
async def logout_handler(request: Request):
    """Handle user logout by revoking the session."""
    try:
        logger.debug("Starting logout process")
        try:
            session = await verify_session(request)
            session_handle = session.get_handle()
            logger.debug(f"Found active session with handle: {session_handle}")
            
            await revoke_session(session_handle)
            logger.debug("Successfully revoked session")
            
            return {"status": "success", "message": "Logged out successfully"}
            
        except Exception as session_error:
            logger.error(f"Session error during logout: {str(session_error)}")
            logger.error(f"Session error traceback: {traceback.format_exc()}")
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired session"
            )
            
    except Exception as e:
        logger.error(f"Error during logout: {str(e)}")
        logger.error(f"Logout error traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to logout: {str(e)}"
        ) 

@router.get("/google")
async def google_auth_handler(request: Request):
    """Handle Google OAuth authentication."""
    try:
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:3001/api/auth/google/callback")
        
        if not client_id:
            raise HTTPException(
                status_code=500,
                detail="Google OAuth credentials not configured"
            )
            
        # Construct Google authorization URL
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": "openid email profile",
            "access_type": "offline",
            "prompt": "consent"
        }
        
        auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
        return {"auth_url": auth_url}
        
    except Exception as e:
        logger.error(f"Error initiating Google auth: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to initiate Google authorization"
        )

@router.get("/google/callback")
async def google_callback_handler(
    request: Request,
    code: str,
    error: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Handle Google OAuth callback."""
    if error:
        logger.error(f"Google authorization error: {error}")
        return RedirectResponse(
            url=f"http://localhost:3001/auth/dashboard?error={error}",
            status_code=303
        )
        
    try:
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:3001/api/auth/google/callback")
        
        if not client_id or not client_secret:
            logger.error("Google credentials not configured")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=configuration_error",
                status_code=303
            )
            
        # Exchange code for token
        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code"
        }
        
        async with httpx.AsyncClient() as client:
            token_response = await client.post(token_url, data=token_data)
            token_info = token_response.json()
            
            if token_response.status_code != 200:
                logger.error(f"Failed to exchange code for token: {token_info}")
                return RedirectResponse(
                    url="http://localhost:3001/auth/dashboard?error=token_exchange_failed",
                    status_code=303
                )
        
        try:
            # Try to get session
            session = await verify_session(request)
            user_id = session.get_user_id()
            
            # Store token in database
            oauth_token = OAuthToken(
                user_id=user_id,
                service="google",
                access_token=token_info["access_token"],
                refresh_token=token_info.get("refresh_token"),
                token_type=token_info.get("token_type", "Bearer"),
                scope=token_info.get("scope", ""),
                expires_at=datetime.utcnow() + timedelta(seconds=token_info.get("expires_in", 3600))
            )
            
            # Update or insert token
            existing_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                service="google"
            ).first()
            
            if existing_token:
                for key, value in oauth_token.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing_token, key, value)
            else:
                db.add(oauth_token)
                
            db.commit()
            
            # Update session with Google connection status
            await session.merge_into_access_token_payload({
                "google_connected": True
            })
            
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?success=true",
                status_code=303
            )
            
        except Exception as e:
            logger.error(f"Error storing token: {str(e)}")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=token_storage_failed",
                status_code=303
            )
        
    except Exception as e:
        logger.error(f"Error handling Google callback: {str(e)}", exc_info=True)
        return RedirectResponse(
            url="http://localhost:3001/auth/dashboard?error=authorization_failed",
            status_code=303
        ) 

@router.get("/linkedin")
async def linkedin_auth_handler(request: Request):
    """Handle LinkedIn OAuth authentication."""
    try:
        client_id = os.getenv("LINKEDIN_CLIENT_ID")
        redirect_uri = os.getenv("LINKEDIN_REDIRECT_URI", "http://localhost:3001/api/auth/linkedin/callback")
        
        if not client_id:
            raise HTTPException(
                status_code=500,
                detail="LinkedIn OAuth credentials not configured"
            )
            
        # Construct LinkedIn authorization URL
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": "profile",  # Most basic scope for LinkedIn v2 API
            "state": "random_state_string"  # In production, this should be a secure random string
        }
        
        auth_url = f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(params)}"
        return {"auth_url": auth_url}
        
    except Exception as e:
        logger.error(f"Error initiating LinkedIn auth: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to initiate LinkedIn authorization"
        )

@router.get("/linkedin/callback")
async def linkedin_callback_handler(
    request: Request,
    code: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Handle LinkedIn OAuth callback."""
    if error:
        error_msg = error_description or error
        logger.error(f"LinkedIn authorization error: {error_msg}")
        return RedirectResponse(
            url=f"http://localhost:3001/auth/dashboard?error={error_msg}",
            status_code=303
        )
        
    if not code:
        logger.error("No authorization code received from LinkedIn")
        return RedirectResponse(
            url="http://localhost:3001/auth/dashboard?error=no_auth_code",
            status_code=303
        )
        
    try:
        client_id = os.getenv("LINKEDIN_CLIENT_ID")
        client_secret = os.getenv("LINKEDIN_CLIENT_SECRET")
        redirect_uri = os.getenv("LINKEDIN_REDIRECT_URI", "http://localhost:3001/api/auth/linkedin/callback")
        
        if not client_id or not client_secret:
            logger.error("LinkedIn credentials not configured")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=configuration_error",
                status_code=303
            )
            
        # Exchange code for token
        token_url = "https://www.linkedin.com/oauth/v2/accessToken"
        token_data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code"
        }
        
        async with httpx.AsyncClient() as client:
            token_response = await client.post(token_url, data=token_data)
            token_info = token_response.json()
            
            if token_response.status_code != 200:
                logger.error(f"Failed to exchange code for token: {token_info}")
                return RedirectResponse(
                    url="http://localhost:3001/auth/dashboard?error=token_exchange_failed",
                    status_code=303
                )
        
        try:
            # Try to get session
            session = await verify_session(request)
            user_id = session.get_user_id()
            
            # Store token in database
            oauth_token = OAuthToken(
                user_id=user_id,
                service="linkedin",
                access_token=token_info["access_token"],
                refresh_token=token_info.get("refresh_token"),  # LinkedIn might not provide refresh token
                token_type=token_info.get("token_type", "Bearer"),
                scope=token_info.get("scope", ""),
                expires_at=datetime.utcnow() + timedelta(seconds=token_info.get("expires_in", 3600))
            )
            
            # Update or insert token
            existing_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                service="linkedin"
            ).first()
            
            if existing_token:
                for key, value in oauth_token.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing_token, key, value)
            else:
                db.add(oauth_token)
                
            db.commit()
            
            # Update session with LinkedIn connection status
            await session.merge_into_access_token_payload({
                "linkedin_connected": True
            })
            
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?success=true",
                status_code=303
            )
            
        except Exception as e:
            logger.error(f"Error storing token: {str(e)}")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=token_storage_failed",
                status_code=303
            )
        
    except Exception as e:
        logger.error(f"Error handling LinkedIn callback: {str(e)}", exc_info=True)
        return RedirectResponse(
            url="http://localhost:3001/auth/dashboard?error=authorization_failed",
            status_code=303
        ) 