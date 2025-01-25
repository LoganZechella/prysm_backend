"""Service for interacting with LinkedIn API and extracting professional traits."""

import os
import logging
from datetime import datetime, timedelta
import httpx
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any

from app.models.oauth import OAuthToken
from app.models.traits import Traits
from app.database import get_db

logger = logging.getLogger(__name__)

class LinkedInService:
    """Service for handling LinkedIn OAuth and trait extraction."""
    
    def __init__(self, db: Session):
        """Initialize the LinkedIn service with database session."""
        self.db = db
        self.client_id = os.getenv("LINKEDIN_CLIENT_ID")
        self.client_secret = os.getenv("LINKEDIN_CLIENT_SECRET")
        
    async def get_client(self, user_id: str) -> Optional[httpx.AsyncClient]:
        """Get an authenticated LinkedIn API client for the user."""
        try:
            token = await self._get_valid_token(user_id)
            if not token:
                logger.warning(f"No valid token found for user {user_id}")
                return None
                
            headers = {
                "Authorization": f"Bearer {token.access_token}",
                "Content-Type": "application/json"
            }
            return httpx.AsyncClient(headers=headers)
            
        except Exception as e:
            logger.error(f"Error getting LinkedIn client: {str(e)}")
            return None
            
    async def _get_valid_token(self, user_id: str) -> Optional[OAuthToken]:
        """Get a valid OAuth token for the user, refreshing if necessary."""
        try:
            token = self.db.query(OAuthToken).filter_by(
                user_id=user_id,
                provider="linkedin"
            ).first()
            
            if not token:
                logger.warning(f"No LinkedIn token found for user {user_id}")
                return None
                
            # LinkedIn tokens are valid for 60 days, no refresh token provided
            if token.expires_at:
                # Convert to naive datetime for comparison
                expires_at = token.expires_at.replace(tzinfo=None)
                if expires_at <= datetime.utcnow():
                    logger.warning(f"LinkedIn token expired for user {user_id}")
                    return None
                
            return token
            
        except Exception as e:
            logger.error(f"Error getting valid token: {str(e)}")
            return None
            
    async def get_profile_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get LinkedIn profile data for a user using OpenID Connect endpoints."""
        try:
            client = await self.get_client(user_id)
            if not client:
                return None
                
            # Get user info from OpenID Connect userinfo endpoint
            userinfo_url = "https://api.linkedin.com/v2/userinfo"
            userinfo_response = await client.get(userinfo_url)
            
            if userinfo_response.status_code != 200:
                logger.error(f"Failed to get userinfo: {userinfo_response.text}")
                return None
                
            userinfo = userinfo_response.json()
            logger.info(f"LinkedIn userinfo response: {userinfo}")  # Debug log
            
            # Extract relevant fields from userinfo response
            profile = {
                "id": userinfo.get("sub"),
                "firstName": userinfo.get("given_name"),
                "lastName": userinfo.get("family_name"),
                "email": userinfo.get("email"),
                "headline": userinfo.get("headline"),
                "picture": userinfo.get("picture"),
                "locale": userinfo.get("locale"),
                "name": userinfo.get("name")
            }
            
            logger.info(f"Extracted profile data: {profile}")  # Debug log
            return profile
            
        except Exception as e:
            logger.error(f"Error getting LinkedIn profile: {str(e)}")
            return None
            
    async def extract_professional_traits(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Extract professional traits from LinkedIn profile data."""
        try:
            profile_data = await self.get_profile_data(user_id)
            if not profile_data:
                return None

            # Structure the traits in a format compatible with our database
            professional_traits = {
                "name": profile_data.get("name"),
                "email": profile_data.get("email"),
                "picture_url": profile_data.get("picture"),
                "locale": profile_data.get("locale", {}).get("country"),
                "language": profile_data.get("locale", {}).get("language"),
                "last_updated": datetime.utcnow().isoformat(),
                "next_update": (datetime.utcnow() + timedelta(days=7)).isoformat()
            }
            
            logger.info(f"Extracted LinkedIn professional traits: {professional_traits}")
            return professional_traits
            
        except Exception as e:
            logger.error(f"Error extracting LinkedIn professional traits: {str(e)}")
            return None 