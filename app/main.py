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
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Add session middleware after CORS
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET_KEY", "your-secret-key-here"),
    same_site="lax",
    https_only=False,
)

# OAuth configuration
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/auth/spotify/callback")

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI", "http://localhost:8000/auth/google/callback")

LINKEDIN_CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET")
LINKEDIN_REDIRECT_URI = os.getenv("LINKEDIN_REDIRECT_URI", "http://localhost:8000/auth/linkedin/callback")

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
        raise HTTPException(status_code=400, detail="Could not retrieve token")
    
    token_data = response.json()
    token_manager.store_token("google", token_data)
    return {"message": "Successfully authenticated with Google"}

# LinkedIn OAuth routes
@app.get("/auth/linkedin")
async def linkedin_login():
    """Initiate LinkedIn OAuth flow"""
    scope = "r_liteprofile r_emailaddress"
    params = {
        "client_id": LINKEDIN_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": LINKEDIN_REDIRECT_URI,
        "scope": scope
    }
    url = "https://www.linkedin.com/oauth/v2/authorization"
    response = RedirectResponse(url=f"{url}?{urlencode(params)}")
    return response

@app.get("/auth/linkedin/callback")
async def linkedin_callback(code: str, request: Request, token_manager: TokenManager = Depends(get_token_manager)):
    """Handle LinkedIn OAuth callback"""
    token_url = "https://www.linkedin.com/oauth/v2/accessToken"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": LINKEDIN_REDIRECT_URI,
        "client_id": LINKEDIN_CLIENT_ID,
        "client_secret": LINKEDIN_CLIENT_SECRET,
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(token_url, data=data)
        
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Could not retrieve token")
    
    token_data = response.json()
    token_manager.store_token("linkedin", token_data)
    return {"message": "Successfully authenticated with LinkedIn"}

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
        raise HTTPException(status_code=500, detail=str(e))

# Logout endpoint
@app.post("/auth/logout")
async def logout(request: Request, token_manager: TokenManager = Depends(get_token_manager)):
    """Clear all authentication tokens"""
    token_manager.clear_all_tokens()
    return {"message": "Successfully logged out of all services"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 