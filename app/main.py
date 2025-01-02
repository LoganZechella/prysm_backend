from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.config import Config
from starlette.middleware.sessions import SessionMiddleware
import os
from dotenv import load_dotenv
from typing import Optional
import httpx
from urllib.parse import urlencode
import logging
from app.utils.auth import TokenManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI(title="Prysm Auth Service")

# Add CORS middleware first
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Set-Cookie", "Access-Control-Allow-Headers", "Access-Control-Allow-Origin", "Authorization"],
    expose_headers=["Set-Cookie"],
)

# Add session middleware after CORS
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET_KEY", "your-secret-key-here"),
    same_site="lax",
    https_only=False,
    max_age=3600,
)

# OAuth configuration
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:3001/api/auth/spotify/callback")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:3001/api/auth/google/callback")

LINKEDIN_CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
LINKEDIN_REDIRECT_URI = os.getenv("LINKEDIN_REDIRECT_URI", "http://localhost:3001/api/auth/linkedin/callback")

def get_token_manager(request: Request) -> TokenManager:
    return TokenManager(request)

# Spotify OAuth routes
@app.get("/auth/spotify")
async def spotify_login():
    """Initiate Spotify OAuth flow"""
    scope = "user-read-private user-read-email playlist-read-private user-top-read user-library-read"
    params = {
        "client_id": SPOTIFY_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": SPOTIFY_REDIRECT_URI,
        "scope": scope
    }
    url = "https://accounts.spotify.com/authorize"
    response = RedirectResponse(url=f"{url}?{urlencode(params)}")
    return response

@app.get("/auth/spotify/callback")
async def spotify_callback(code: str, request: Request, token_manager: TokenManager = Depends(get_token_manager)):
    """Handle Spotify OAuth callback"""
    try:
        token_url = "https://accounts.spotify.com/api/token"
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": SPOTIFY_REDIRECT_URI,
            "client_id": SPOTIFY_CLIENT_ID,
            "client_secret": SPOTIFY_CLIENT_SECRET,
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(token_url, data=data, headers=headers)
            
        if response.status_code != 200:
            logger.error(f"Spotify token error: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Could not retrieve token: {response.text}"
            )
        
        token_data = response.json()
        logger.info("Received token data from Spotify")
        
        token_manager.store_token("spotify", token_data)
        
        # Use status code 303 for POST-redirect-GET pattern
        response = RedirectResponse(url="http://localhost:3001", status_code=303)
        return response
        
    except Exception as e:
        logger.error(f"Error in spotify_callback: {str(e)}")
        # Redirect to frontend with error
        return RedirectResponse(
            url=f"http://localhost:3001?error={str(e)}",
            status_code=303
        )

# Google OAuth routes
@app.get("/auth/google")
async def google_login():
    """Initiate Google OAuth flow"""
    scope = "https://www.googleapis.com/auth/userinfo.profile https://www.googleapis.com/auth/userinfo.email"
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "scope": scope
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth"
    response = RedirectResponse(url=f"{url}?{urlencode(params)}")
    return response

@app.get("/auth/google/callback")
async def google_callback(code: str, request: Request, token_manager: TokenManager = Depends(get_token_manager)):
    """Handle Google OAuth callback"""
    try:
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": GOOGLE_REDIRECT_URI
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(token_url, data=data)
            
        if response.status_code != 200:
            logger.error(f"Google token error: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Could not retrieve token: {response.text}"
            )
        
        token_data = response.json()
        token_manager.store_token("google", token_data)
        
        # Redirect back to frontend
        return RedirectResponse(
            url="http://localhost:3001",
            status_code=303
        )
        
    except Exception as e:
        logger.error(f"Error in google_callback: {str(e)}")
        # Redirect to frontend with error
        return RedirectResponse(
            url=f"http://localhost:3001?error={str(e)}",
            status_code=303
        )

# LinkedIn OAuth routes
@app.get("/auth/linkedin")
async def linkedin_login(request: Request):
    """Initiate LinkedIn OAuth flow"""
    try:
        # Use OpenID Connect scopes
        scope = "openid profile email"
        # Generate a random state for security
        import secrets
        state = secrets.token_urlsafe(16)
        
        params = {
            "response_type": "code",
            "client_id": LINKEDIN_CLIENT_ID,
            "redirect_uri": LINKEDIN_REDIRECT_URI,
            "state": state,
            "scope": scope
        }
        
        # Use v2 authorization endpoint
        url = "https://www.linkedin.com/oauth/v2/authorization"
        auth_url = f"{url}?{urlencode(params)}"
        logger.info(f"Initiating LinkedIn OAuth flow with state: {state}")
        
        # Store state in session for verification
        request.session["linkedin_oauth_state"] = state
        
        return RedirectResponse(url=auth_url)
    except Exception as e:
        error_msg = f"Error initiating LinkedIn OAuth: {str(e)}"
        logger.error(error_msg)
        return RedirectResponse(
            url=f"http://localhost:3001?error={error_msg}",
            status_code=303
        )

@app.get("/auth/linkedin/callback")
async def linkedin_callback(
    request: Request,
    code: Optional[str] = None,
    error: Optional[str] = None,
    state: Optional[str] = None,
    token_manager: TokenManager = Depends(get_token_manager)
):
    """Handle LinkedIn OAuth callback"""
    try:
        if error:
            logger.error(f"LinkedIn OAuth error: {error}")
            return RedirectResponse(
                url=f"http://localhost:3001?error={error}",
                status_code=303
            )

        if not code:
            logger.error("No code parameter in LinkedIn callback")
            return RedirectResponse(
                url="http://localhost:3001?error=No authorization code received",
                status_code=303
            )

        # Verify state parameter
        stored_state = request.session.get("linkedin_oauth_state")
        if not state or state != stored_state:
            error_msg = "Invalid state parameter"
            logger.error(f"{error_msg}. Expected: {stored_state}, Got: {state}")
            return RedirectResponse(
                url=f"http://localhost:3001?error={error_msg}",
                status_code=303
            )

        # Clear state from session
        request.session.pop("linkedin_oauth_state", None)

        # Use v2 token endpoint
        token_url = "https://www.linkedin.com/oauth/v2/accessToken"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "LinkedIn-Version": "202304"  # Add API version header
        }
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": LINKEDIN_REDIRECT_URI,
            "client_id": LINKEDIN_CLIENT_ID,
            "client_secret": LINKEDIN_CLIENT_SECRET,
        }
        
        logger.info("Requesting LinkedIn access token")
        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data=data,
                headers=headers
            )
            
        if response.status_code != 200:
            error_msg = f"LinkedIn token error: {response.status_code} - {response.text}"
            logger.error(error_msg)
            return RedirectResponse(
                url=f"http://localhost:3001?error={error_msg}",
                status_code=303
            )
        
        token_data = response.json()
        logger.info("Successfully obtained LinkedIn token")
        token_manager.store_token("linkedin", token_data)
        
        return RedirectResponse(
            url="http://localhost:3001",
            status_code=303
        )
        
    except Exception as e:
        error_msg = f"Error in linkedin_callback: {str(e)}"
        logger.error(error_msg)
        return RedirectResponse(
            url=f"http://localhost:3001?error={error_msg}",
            status_code=303
        )

# Status endpoint
@app.get("/auth/status")
async def auth_status(request: Request, token_manager: TokenManager = Depends(get_token_manager)):
    """Check authentication status for all services"""
    try:
        status = {
            "spotify": token_manager.is_token_valid("spotify"),
            "google": token_manager.is_token_valid("google"),
            "linkedin": token_manager.is_token_valid("linkedin")
        }
        logger.info(f"Auth status: {status}")
        return JSONResponse(content=status)
    except Exception as e:
        logger.error(f"Error checking auth status: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )

# Logout endpoint
@app.post("/auth/logout")
async def logout(request: Request, token_manager: TokenManager = Depends(get_token_manager)):
    """Clear all authentication tokens"""
    token_manager.clear_all_tokens()
    return {"message": "Successfully logged out of all services"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 