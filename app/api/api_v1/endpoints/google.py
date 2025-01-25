"""Google OAuth endpoints."""
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import httpx

from app.core.config import settings
from app.db.session import get_db
from app.services.google import GooglePeopleService
from app.models.oauth import OAuthToken
from app.utils.logging import setup_logger

router = APIRouter()
logger = setup_logger(__name__)

@router.get("/auth/google/login")
async def google_login():
    """Start Google OAuth flow."""
    logger.info(f"Using redirect URI: {settings.GOOGLE_REDIRECT_URI}")
    auth_url = (
        "https://accounts.google.com/o/oauth2/v2/auth?"
        f"client_id={settings.GOOGLE_CLIENT_ID}&"
        "response_type=code&"
        f"redirect_uri={settings.GOOGLE_REDIRECT_URI}&"
        "scope=" + "%20".join([
            'https://www.googleapis.com/auth/userinfo.profile',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/contacts.readonly',
            'https://www.googleapis.com/auth/user.organization.read',
            'https://www.googleapis.com/auth/user.emails.read',
            'https://www.googleapis.com/auth/user.addresses.read'
        ])
    )
    logger.info(f"Generated auth URL: {auth_url}")
    return {"auth_url": auth_url}

@router.get("/auth/google/callback")
async def google_callback(
    request: Request,
    code: str,
    db: Session = Depends(get_db)
):
    """Handle Google OAuth callback."""
    try:
        # Exchange code for tokens
        async with httpx.AsyncClient() as client:
            token_response = await client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": settings.GOOGLE_CLIENT_ID,
                    "client_secret": settings.GOOGLE_CLIENT_SECRET,
                    "code": code,
                    "grant_type": "authorization_code",
                    "redirect_uri": settings.GOOGLE_REDIRECT_URI
                }
            )
            token_data = token_response.json()
            
            if "error" in token_data:
                logger.error(f"Google OAuth error: {token_data}")
                raise HTTPException(status_code=400, detail="Failed to get access token")
                
            # Get user info to get user ID
            headers = {"Authorization": f"Bearer {token_data['access_token']}"}
            user_info_response = await client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers=headers
            )
            user_info = user_info_response.json()
            
            if "error" in user_info:
                logger.error(f"Google user info error: {user_info}")
                raise HTTPException(status_code=400, detail="Failed to get user info")
            
            # Store token in database
            token = OAuthToken(
                user_id=user_info["id"],  # or however you want to map the user ID
                provider="google",
                client_id=settings.GOOGLE_CLIENT_ID,
                client_secret=settings.GOOGLE_CLIENT_SECRET,
                redirect_uri=settings.GOOGLE_REDIRECT_URI,
                access_token=token_data["access_token"],
                refresh_token=token_data.get("refresh_token"),
                token_type=token_data["token_type"],
                scope=token_data["scope"],
                expires_at=datetime.utcnow() + timedelta(seconds=token_data["expires_in"]),
                provider_metadata={
                    "email": user_info.get("email"),
                    "name": user_info.get("name"),
                    "picture": user_info.get("picture")
                }
            )
            
            # Check if token already exists
            existing_token = (
                db.query(OAuthToken)
                .filter(
                    OAuthToken.user_id == user_info["id"],
                    OAuthToken.provider == "google"
                )
                .first()
            )
            
            if existing_token:
                # Update existing token
                for key, value in token.__dict__.items():
                    if not key.startswith("_"):
                        setattr(existing_token, key, value)
            else:
                db.add(token)
                
            db.commit()
            
            return {
                "message": "Successfully authenticated with Google",
                "user_id": user_info["id"]
            }
            
    except Exception as e:
        logger.error(f"Error in Google callback: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/auth/google/profile/{user_id}")
async def get_google_profile(
    user_id: str,
    db: Session = Depends(get_db)
):
    """Get user's Google profile data."""
    try:
        google_service = GooglePeopleService(db)
        profile_data = await google_service.get_profile_data(user_id)
        
        if not profile_data:
            raise HTTPException(status_code=404, detail="Profile data not found")
            
        return profile_data
        
    except Exception as e:
        logger.error(f"Error getting Google profile: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/auth/google/traits/{user_id}")
async def get_google_traits(
    user_id: str,
    db: Session = Depends(get_db)
):
    """Get user's professional traits from Google data."""
    try:
        google_service = GooglePeopleService(db)
        traits = await google_service.extract_professional_traits(user_id)
        
        if not traits:
            raise HTTPException(status_code=404, detail="Traits not found")
            
        return traits
        
    except Exception as e:
        logger.error(f"Error getting Google traits: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error") 