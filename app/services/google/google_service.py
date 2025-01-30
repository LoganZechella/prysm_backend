"""Service for interacting with Google People API."""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from app.models.oauth import OAuthToken

logger = logging.getLogger(__name__)

class GooglePeopleService:
    """Service for handling Google People API and trait extraction."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self._client = None
        
    async def get_client(self, user_id: str):
        """Get an authenticated Google People API client for a user."""
        try:
            logger.info(f"Getting valid token for user {user_id}")
            token = await self._get_valid_token(user_id)
            if not token:
                logger.warning(f"No valid token found for user {user_id}")
                return None
                
            creds = Credentials(
                token=token.access_token,
                refresh_token=token.refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=token.client_id,
                client_secret=token.client_secret,
                scopes=token.scope.split()
            )
            
            if not creds.valid:
                if creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    # Update token in database
                    token.access_token = creds.token
                    token.expires_at = datetime.utcnow() + timedelta(seconds=creds.expiry.timestamp() - datetime.utcnow().timestamp())
                    await self.db.commit()
                else:
                    logger.warning("Credentials not valid and cannot be refreshed")
                    return None
            
            return build('people', 'v1', credentials=creds)
            
        except Exception as e:
            logger.error(f"Error getting Google client: {str(e)}")
            return None
            
    async def get_professional_traits(self, user_id: str) -> Dict[str, Any]:
        """Get professional traits for a user."""
        try:
            client = await self.get_client(user_id)
            if not client:
                logger.error(f"Could not get Google client for user {user_id}")
                return {}
                
            return await self._extract_professional_traits(client)
            
        except Exception as e:
            logger.error(f"Error getting professional traits: {str(e)}")
            return {}
            
    async def _extract_professional_traits(self, client) -> Dict[str, Any]:
        """Extract professional traits from Google profile data."""
        try:
            # Get profile data with valid fields
            profile = client.people().get(
                resourceName='people/me',
                personFields='names,emailAddresses,organizations,occupations,locations,interests,skills,urls'
            ).execute()
            
            # Extract relevant information
            traits = {
                "skills": [],
                "interests": [],
                "locations": [],
                "organizations": [],
                "occupations": []
            }
            
            # Process skills
            if 'skills' in profile:
                traits["skills"] = [skill.get('value') for skill in profile['skills']]
                
            # Process interests
            if 'interests' in profile:
                traits["interests"] = [interest.get('value') for interest in profile['interests']]
                
            # Process locations
            if 'locations' in profile:
                for location in profile['locations']:
                    if 'current' in location and location['current']:
                        loc_data = {
                            "city": location.get('city'),
                            "state": location.get('region'),
                            "country": location.get('country'),
                            "formatted": location.get('value')
                        }
                        traits["locations"].append(loc_data)
                        
            # Process organizations
            if 'organizations' in profile:
                for org in profile['organizations']:
                    org_data = {
                        "name": org.get('name'),
                        "title": org.get('title'),
                        "type": org.get('type')
                    }
                    traits["organizations"].append(org_data)
                    
            # Process occupations
            if 'occupations' in profile:
                traits["occupations"] = [occ.get('value') for occ in profile['occupations']]
            
            return traits
            
        except Exception as e:
            logger.error(f"Error extracting professional traits: {str(e)}")
            return {}
            
    async def _get_valid_token(self, user_id: str) -> Optional[OAuthToken]:
        """Get a valid OAuth token for the user, refreshing if necessary."""
        try:
            result = await self.db.execute(
                select(OAuthToken).filter_by(
                    user_id=user_id,
                    provider="google"
                )
            )
            token = result.scalar_one_or_none()
            
            if not token:
                logger.warning(f"No Google token found for user {user_id}")
                return None
                
            if token.expires_at:
                # Convert to naive datetime for comparison
                expires_at = token.expires_at.replace(tzinfo=None)
                if expires_at <= datetime.utcnow():
                    logger.warning(f"Google token expired for user {user_id}")
                    return None
                
            return token
            
        except Exception as e:
            logger.error(f"Error getting valid token: {str(e)}")
            return None 