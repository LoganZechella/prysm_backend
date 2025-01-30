"""LinkedIn OAuth endpoints."""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session
import httpx
import logging
from urllib.parse import urlencode
import secrets
from fastapi.responses import RedirectResponse

from app.core.config import settings
from app.database import get_db
from app.models.oauth import OAuthToken
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
router = APIRouter()

# Get credentials from settings
CLIENT_ID = settings.LINKEDIN_CLIENT_ID
CLIENT_SECRET = settings.LINKEDIN_CLIENT_SECRET
REDIRECT_URI = settings.LINKEDIN_REDIRECT_URI

logger.info(f"Using LinkedIn credentials - Client ID: {CLIENT_ID}")

@router.get("/auth/linkedin")
async def linkedin_login():
    """Initiate LinkedIn OAuth flow."""
    # Generate state for CSRF protection
    state = secrets.token_urlsafe(32)
    
    # Construct LinkedIn authorization URL
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "scope": "profile"  # Use only the profile scope that we have authorized
    }
    
    auth_url = f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(params)}"
    return RedirectResponse(url=auth_url)

@router.get("/auth/linkedin/callback")
async def linkedin_callback(
    request: Request,
    response: Response,
    code: str,
    state: str,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Handle LinkedIn OAuth callback."""
    try:
        logger.info("Received LinkedIn callback")
        # Exchange code for token using OpenID Connect endpoint
        token_url = "https://www.linkedin.com/oauth/v2/accessToken"
        
        # Construct form data manually to ensure exact format
        form_data = (
            f"grant_type=authorization_code&"
            f"code={code}&"
            f"client_id={CLIENT_ID}&"
            f"client_secret={CLIENT_SECRET}&"
            f"redirect_uri={REDIRECT_URI}"
        )
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        
        logger.info("Token exchange request details:")
        logger.info(f"URL: {token_url}")
        logger.info(f"Headers: {headers}")
        logger.info(f"Form data: {form_data}")
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    token_url,
                    headers=headers,
                    content=form_data,  # Use content instead of data
                    follow_redirects=True
                )
                
                if not response.is_success:
                    error_data = response.json()
                    logger.error(f"Token exchange failed with status {response.status_code}")
                    logger.error(f"Error data: {error_data}")
                    logger.error(f"Request that was sent: {form_data}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to exchange code for token: {error_data}"
                    )
                    
                token_data = response.json()
                logger.info("Successfully obtained token")
            except Exception as e:
                logger.error(f"Exception during token exchange: {str(e)}")
                raise
        
        # Create token in database
        token = OAuthToken(
            user_id="test_user",  # Using test user for now
            provider="linkedin",
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri=REDIRECT_URI,
            access_token=token_data["access_token"],
            token_type=token_data.get("token_type", "Bearer"),
            scope=token_data.get("scope", ""),
            expires_at=datetime.utcnow() + timedelta(seconds=token_data.get("expires_in", 5184000)),  # Default to 60 days
            provider_metadata=token_data
        )
        
        # Save to database
        db.add(token)
        db.commit()
        db.refresh(token)
        logger.info("Token saved to database")
        
        return {"message": "LinkedIn OAuth token created successfully"}
        
    except Exception as e:
        logger.error(f"Error in LinkedIn callback: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating LinkedIn token: {str(e)}"
        ) 