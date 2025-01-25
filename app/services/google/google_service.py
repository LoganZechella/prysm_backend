"""Service for handling Google People API OAuth and trait extraction."""
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from sqlalchemy.orm import Session
from google.auth.transport.requests import Request

from app.models.oauth import OAuthToken
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

class GooglePeopleService:
    """Service for handling Google People API OAuth and trait extraction."""
    
    def __init__(self, db: Session):
        self.db = db
        self._client = None
        self.scopes = [
            'https://www.googleapis.com/auth/userinfo.profile',
            'https://www.googleapis.com/auth/userinfo.email',
            'https://www.googleapis.com/auth/contacts.readonly',
            'https://www.googleapis.com/auth/user.organization.read',
            'https://www.googleapis.com/auth/user.emails.read',
            'https://www.googleapis.com/auth/user.addresses.read'
        ]
    
    async def get_client(self, user_id: str) -> Optional[Any]:
        """Get an authenticated Google People API client for a user."""
        try:
            logger.info(f"Getting valid token for user {user_id}")
            token = await self._get_valid_token(user_id)
            if not token:
                logger.warning(f"No valid token found for user {user_id}")
                return None
            
            credentials = Credentials(
                token=token.access_token,
                refresh_token=token.refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=token.client_id,
                client_secret=token.client_secret,
                scopes=token.scope.split()
            )
            
            return build('people', 'v1', credentials=credentials)
            
        except Exception as e:
            logger.error(f"Error getting Google client: {str(e)}")
            return None
    
    async def _get_valid_token(self, user_id: str) -> Optional[OAuthToken]:
        """Get a valid OAuth token for the user, refreshing if necessary."""
        token = (
            self.db.query(OAuthToken)
            .filter(
                OAuthToken.user_id == user_id,
                OAuthToken.provider == "google"
            )
            .first()
        )
        
        if not token:
            return None
            
        # Convert expires_at to UTC for comparison
        now = datetime.utcnow()
        expires_at = token.expires_at.replace(tzinfo=None)
            
        # Check if token needs refresh
        if expires_at <= now + timedelta(minutes=5):
            try:
                credentials = Credentials(
                    token=token.access_token,
                    refresh_token=token.refresh_token,
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=token.client_id,
                    client_secret=token.client_secret,
                    scopes=token.scope.split()
                )
                
                # Refresh the token
                request = Request()
                credentials.refresh(request)
                
                # Update token in database
                token.access_token = credentials.token
                token.expires_at = datetime.utcnow() + timedelta(seconds=credentials.expiry.timestamp() - datetime.utcnow().timestamp())
                token.last_used_at = datetime.utcnow()
                self.db.commit()
                
            except Exception as e:
                logger.error(f"Error refreshing token: {str(e)}")
                return None
                
        return token
    
    async def get_profile_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user's profile data from Google People API."""
        client = await self.get_client(user_id)
        if not client:
            return None
            
        try:
            # Get person data
            person = client.people().get(
                resourceName='people/me',
                personFields=','.join([
                    'names',
                    'emailAddresses',
                    'organizations',
                    'occupations',
                    'locations',
                    'interests',
                    'skills',
                    'urls'
                ])
            ).execute()
            
            return {
                'names': person.get('names', []),
                'emailAddresses': person.get('emailAddresses', []),
                'organizations': person.get('organizations', []),
                'occupations': person.get('occupations', []),
                'locations': person.get('locations', []),
                'interests': person.get('interests', []),
                'skills': person.get('skills', []),
                'urls': person.get('urls', [])
            }
            
        except Exception as e:
            logger.error(f"Error getting profile data: {str(e)}")
            return None
    
    async def extract_professional_traits(self, user_id: str) -> Dict[str, Any]:
        """Extract professional traits from Google People API data."""
        profile_data = await self.get_profile_data(user_id)
        if not profile_data:
            return {}
            
        professional_traits = {
            'organizations': [
                {
                    'name': org.get('name', ''),
                    'title': org.get('title', ''),
                    'type': org.get('type', ''),
                    'startDate': org.get('startDate', {}),
                    'endDate': org.get('endDate', {})
                }
                for org in profile_data.get('organizations', [])
            ],
            'occupations': [
                occupation.get('value', '')
                for occupation in profile_data.get('occupations', [])
            ],
            'locations': [
                location.get('value', '')
                for location in profile_data.get('locations', [])
            ],
            'interests': [
                interest.get('value', '')
                for interest in profile_data.get('interests', [])
            ],
            'skills': [
                skill.get('value', '')
                for skill in profile_data.get('skills', [])
            ],
            'urls': [
                {
                    'value': url.get('value', ''),
                    'type': url.get('type', '')
                }
                for url in profile_data.get('urls', [])
            ]
        }
        
        return professional_traits 