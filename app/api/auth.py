from fastapi import APIRouter, HTTPException, Request, Depends
from typing import Optional
from pydantic import BaseModel
from app.auth import get_current_user, create_access_token, SECRET_KEY_BYTES, ALGORITHM
from app.models.oauth import OAuthToken
from app.database import get_db
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import logging
import os
from urllib.parse import urlencode
import spotipy
from fastapi.responses import RedirectResponse, HTMLResponse
from supertokens_python.recipe.session.asyncio import revoke_session, create_new_session, get_session
import httpx
import traceback
import jwt

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter()

class AuthStatus(BaseModel):
    spotify: bool = False
    google: bool = False
    linkedin: bool = False

async def verify_session(request: Request):
    """Verify and get the current session."""
    try:
        session = await get_session(request)
        if not session:
            raise HTTPException(
                status_code=401,
                detail="No valid session found"
            )
        return session
    except Exception as e:
        logger.error(f"Error verifying session: {str(e)}")
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired session"
        )

@router.get("/status")
async def auth_status_handler(
    request: Request,
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get the current authentication status."""
    try:
        logger.debug(f"Checking auth status for user: {user_id}")
        logger.debug(f"Request headers: {request.headers}")
        
        # Check each service's connection status
        spotify_token = db.query(OAuthToken).filter_by(
            user_id=user_id,
            provider="spotify"
        ).first()
        
        google_token = db.query(OAuthToken).filter_by(
            user_id=user_id,
            provider="google"
        ).first()
        
        linkedin_token = db.query(OAuthToken).filter_by(
            user_id=user_id,
            provider="linkedin"
        ).first()
        
        status = AuthStatus(
            spotify=bool(spotify_token),
            google=bool(google_token),
            linkedin=bool(linkedin_token)
        )
        logger.debug(f"Auth status for user {user_id}: {status}")
        return status
            
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
            user_id = await get_current_user(request)
            logger.debug(f"Found existing session for user: {user_id}")
            # Even for existing sessions, return a fresh token
            token = create_access_token(user_id)
            logger.debug(f"Created fresh token for existing user: {token}")
            return {"status": "success", "message": "Session exists", "token": token}
        except Exception as e:
            logger.debug(f"No existing session found, creating new one. Error: {str(e)}")
            # Create a new session with a temporary user ID
            temp_user_id = f"temp_{datetime.utcnow().timestamp()}"
            token = create_access_token(temp_user_id)
            logger.debug(f"Created new token for temp user: {token}")
            return {"status": "success", "message": "Session created", "token": token}
            
    except Exception as e:
        logger.error(f"Error initializing session: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to initialize session"
        )

@router.get("/spotify")
async def spotify_auth_handler(request: Request):
    """Handle Spotify OAuth authorization."""
    try:
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/v1/auth/spotify/callback")
        scope = " ".join([
            'user-read-private',
            'user-read-email',
            'user-top-read',
            'user-read-recently-played',
            'playlist-read-private',
            'playlist-read-collaborative',
            'user-library-read',
            'user-follow-read',
            'user-follow-modify'
        ])
        
        if not client_id:
            raise HTTPException(
                status_code=500,
                detail="Spotify client ID not configured"
            )

        # Get user_id from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="No valid token provided"
            )

        token = auth_header.split(" ")[1]
        
        # Construct Spotify authorization URL
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scope,
            "show_dialog": True,
            "state": token  # Pass the JWT token as state
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
    state: Optional[str] = None,
    error: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Handle Spotify OAuth callback."""
    if error:
        logger.error(f"Spotify authorization error: {error}")
        raise HTTPException(
            status_code=400,
            detail=f"Spotify authorization error: {error}"
        )
        
    try:
        # Exchange the authorization code for access token
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/api/v1/auth/spotify/callback")
        
        if not client_id or not client_secret:
            logger.error("Spotify credentials not configured")
            raise HTTPException(
                status_code=500,
                detail="Spotify credentials not configured"
            )
            
        # Create Spotify OAuth client
        auth = spotipy.oauth2.SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=" ".join([
                'user-read-private',
                'user-read-email',
                'user-top-read',
                'user-read-recently-played',
                'playlist-read-private',
                'playlist-read-collaborative',
                'user-library-read',
                'user-follow-read',
                'user-follow-modify'
            ])
        )
        
        # Exchange code for token
        token_info = auth.get_access_token(code)
        
        if not token_info:
            logger.error("Failed to exchange code for token")
            raise HTTPException(
                status_code=500,
                detail="Failed to exchange code for token"
            )
        
        try:
            # Get user_id from state parameter
            if not state:
                logger.error("No state parameter found")
                raise HTTPException(
                    status_code=400,
                    detail="No state parameter found"
                )

            try:
                payload = jwt.decode(state, SECRET_KEY_BYTES, algorithms=[ALGORITHM])
                user_id = payload.get("sub")
                if not user_id:
                    raise jwt.InvalidTokenError("No user_id in token")
            except jwt.InvalidTokenError as e:
                logger.error(f"Invalid state token: {str(e)}")
                raise HTTPException(
                    status_code=401,
                    detail="Invalid state token"
                )
            
            # Store token in database
            oauth_token = OAuthToken(
                user_id=user_id,
                provider="spotify",
                access_token=token_info["access_token"],
                refresh_token=token_info.get("refresh_token"),
                expires_at=datetime.utcnow() + timedelta(seconds=token_info.get("expires_in", 3600)),
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                scope=token_info.get("scope", "")
            )
            
            # Update or insert token
            existing_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                provider="spotify"
            ).first()
            
            if existing_token:
                for key, value in oauth_token.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing_token, key, value)
            else:
                db.add(oauth_token)
                
            db.commit()
            logger.info(f"Successfully stored Spotify token for user {user_id}")
            
            return {
                "status": "success",
                "message": "Successfully authenticated with Spotify",
                "user_id": user_id
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error storing token: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to store token"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error handling Spotify callback: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to handle Spotify callback"
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
    """Handle Google OAuth authorization."""
    try:
        client_id = os.getenv("GOOGLE_CLIENT_ID")
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/api/auth/google/callback")
        
        if not client_id:
            raise HTTPException(
                status_code=500,
                detail="Google OAuth credentials not configured"
            )

        # Get user_id from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="No valid token provided"
            )

        token = auth_header.split(" ")[1]
            
        # Construct Google authorization URL
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": "openid email profile",
            "access_type": "offline",
            "prompt": "consent",
            "state": token  # Pass the JWT token as state
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
    state: Optional[str] = None,
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
        redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/api/auth/google/callback")
        
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
            # Get user_id from state parameter
            if not state:
                logger.error("No state parameter found")
                return RedirectResponse(
                    url="http://localhost:3001/auth/dashboard?error=no_session",
                    status_code=303
                )

            try:
                payload = jwt.decode(state, SECRET_KEY_BYTES, algorithms=[ALGORITHM])
                user_id = payload.get("sub")
                if not user_id:
                    raise jwt.InvalidTokenError("No user_id in token")
            except jwt.InvalidTokenError as e:
                logger.error(f"Invalid state token: {str(e)}")
                return RedirectResponse(
                    url="http://localhost:3001/auth/dashboard?error=invalid_session",
                    status_code=303
                )
            
            # Store token in database
            oauth_token = OAuthToken(
                user_id=user_id,
                provider="google",
                access_token=token_info["access_token"],
                refresh_token=token_info.get("refresh_token"),
                expires_at=datetime.utcnow() + timedelta(seconds=token_info.get("expires_in", 3600))
            )
            
            # Update or insert token
            existing_token = db.query(OAuthToken).filter_by(
                user_id=user_id,
                provider="google"
            ).first()
            
            if existing_token:
                for key, value in oauth_token.__dict__.items():
                    if not key.startswith('_'):
                        setattr(existing_token, key, value)
            else:
                db.add(oauth_token)
                
            db.commit()
            logger.info(f"Successfully stored Google token for user {user_id}")
            
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
        redirect_uri = os.getenv("LINKEDIN_REDIRECT_URI", "http://localhost:8000/api/v1/auth/linkedin/callback")
        
        if not client_id:
            raise HTTPException(
                status_code=500,
                detail="LinkedIn OAuth credentials not configured"
            )
            
        # Get user_id from Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="No valid token provided"
            )

        token = auth_header.split(" ")[1]
            
        # Construct LinkedIn authorization URL with correct scopes
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": "openid profile email",  # Use authorized scopes from Developer Console
            "state": token  # Pass the JWT token as state
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
    state: Optional[str] = None,
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
        redirect_uri = os.getenv("LINKEDIN_REDIRECT_URI", "http://localhost:8000/api/v1/auth/linkedin/callback")
        
        if not client_id or not client_secret:
            logger.error("LinkedIn credentials not configured")
            return RedirectResponse(
                url="http://localhost:3001/auth/dashboard?error=configuration_error",
                status_code=303
            )

        # For testing purposes, use test_user
        user_id = "test_user"
            
        # Exchange code for token
        token_url = "https://www.linkedin.com/oauth/v2/accessToken"
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri
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
            
            logger.info(f"Successfully obtained token: {token_info}")
            
            # Store token in database
            try:
                oauth_token = OAuthToken(
                    user_id=user_id,
                    provider="linkedin",
                    access_token=token_info["access_token"],
                    token_type=token_info.get("token_type", "Bearer"),
                    scope="openid profile email",  # Match the scope used in authorization
                    expires_at=datetime.utcnow() + timedelta(seconds=token_info.get("expires_in", 3600)),
                    client_id=client_id,
                    client_secret=client_secret,
                    redirect_uri=redirect_uri
                )
                
                # Update or insert token
                existing_token = db.query(OAuthToken).filter_by(
                    user_id=user_id,
                    provider="linkedin"
                ).first()
                
                if existing_token:
                    existing_token.access_token = oauth_token.access_token
                    existing_token.token_type = oauth_token.token_type
                    existing_token.scope = oauth_token.scope
                    existing_token.expires_at = oauth_token.expires_at
                    existing_token.client_id = oauth_token.client_id
                    existing_token.client_secret = oauth_token.client_secret
                    existing_token.redirect_uri = oauth_token.redirect_uri
                else:
                    db.add(oauth_token)
                    
                db.commit()
                logger.info(f"Successfully stored LinkedIn token for user {user_id}")
                
                # Return HTML success page instead of redirecting
                html_content = """
                <!DOCTYPE html>
                <html>
                    <head>
                        <title>LinkedIn Authorization Success</title>
                        <style>
                            body { font-family: Arial, sans-serif; text-align: center; padding-top: 50px; }
                            .success { color: #4CAF50; }
                        </style>
                    </head>
                    <body>
                        <h1 class="success">✓ Authorization Successful!</h1>
                        <p>Your LinkedIn account has been successfully connected.</p>
                        <p>You can close this window now.</p>
                    </body>
                </html>
                """
                return HTMLResponse(content=html_content, status_code=200)
            except Exception as db_error:
                logger.error(f"Database error: {str(db_error)}")
                html_error = """
                <!DOCTYPE html>
                <html>
                    <head>
                        <title>LinkedIn Authorization Error</title>
                        <style>
                            body { font-family: Arial, sans-serif; text-align: center; padding-top: 50px; }
                            .error { color: #f44336; }
                        </style>
                    </head>
                    <body>
                        <h1 class="error">× Authorization Failed</h1>
                        <p>There was an error storing your LinkedIn credentials.</p>
                        <p>Please try again later.</p>
                    </body>
                </html>
                """
                return HTMLResponse(content=html_error, status_code=500)
            
    except Exception as e:
        logger.error(f"Error handling LinkedIn callback: {str(e)}", exc_info=True)
        html_error = """
        <!DOCTYPE html>
        <html>
            <head>
                <title>LinkedIn Authorization Error</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding-top: 50px; }
                    .error { color: #f44336; }
                </style>
            </head>
            <body>
                <h1 class="error">× Authorization Failed</h1>
                <p>There was an error processing your LinkedIn authorization.</p>
                <p>Please try again later.</p>
            </body>
        </html>
        """
        return HTMLResponse(content=html_error, status_code=500)

@router.post("/session/refresh")
async def refresh_session_handler(request: Request):
    """Refresh the user's session token."""
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail="No token provided"
            )
        
        old_token = auth_header.split(" ")[1]
        
        try:
            # First try to decode without verifying expiration
            payload = jwt.decode(old_token, SECRET_KEY_BYTES, algorithms=[ALGORITHM], options={"verify_exp": False})
            user_id = payload.get("sub")
            
            if not user_id:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid token"
                )
            
            try:
                # Now try to verify the token fully
                jwt.decode(old_token, SECRET_KEY_BYTES, algorithms=[ALGORITHM])
                # If we get here, token is still valid
                return {"message": "Token still valid"}
            except jwt.ExpiredSignatureError:
                # Token is expired but valid, create new one
                new_token = create_access_token(user_id)
                return {"access_token": new_token, "token_type": "bearer"}
            
        except jwt.InvalidTokenError:
            raise HTTPException(
                status_code=401,
                detail="Invalid token"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing session: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to refresh session"
        ) 