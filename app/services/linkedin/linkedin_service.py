"""Service for interacting with LinkedIn API and extracting professional traits."""

import os
import logging
from datetime import datetime, timedelta
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, Dict, Any

from app.models.oauth import OAuthToken
from app.models.traits import Traits
from app.database import get_db

logger = logging.getLogger(__name__)

class LinkedInService:
    """Service for handling LinkedIn API and trait extraction."""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self._client = None
        self.scopes = [
            'r_liteprofile',
            'r_emailaddress',
            'r_basicprofile',
            'r_organization_social'
        ]
        
    async def get_client(self, user_id: str) -> Optional[httpx.AsyncClient]:
        """Get an authenticated LinkedIn API client for a user."""
        try:
            token = await self._get_valid_token(user_id)
            if not token:
                logger.warning(f"No valid token found for user {user_id}")
                return None
                
            return httpx.AsyncClient(
                base_url="https://api.linkedin.com/v2",
                headers={
                    "Authorization": f"Bearer {token.access_token}",
                    "X-Restli-Protocol-Version": "2.0.0"
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting LinkedIn client: {str(e)}")
            return None
            
    async def get_professional_traits(self, user_id: str) -> Dict[str, Any]:
        """Get professional traits for a user."""
        try:
            client = await self.get_client(user_id)
            if not client:
                logger.error(f"Could not get LinkedIn client for user {user_id}")
                return {}
                
            return await self._extract_professional_traits(client)
            
        except Exception as e:
            logger.error(f"Error getting professional traits: {str(e)}")
            return {}
            
    async def get_profile_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user's profile data from LinkedIn API."""
        client = await self.get_client(user_id)
        if not client:
            return None
            
        try:
            # Get basic profile
            profile_response = await client.get("/me")
            profile_response.raise_for_status()
            profile = profile_response.json()
            
            # Get email address
            email_response = await client.get("/emailAddress?q=members&projection=(elements*(handle~))")
            email_response.raise_for_status()
            email_data = email_response.json()
            
            # Get positions
            positions_response = await client.get("/positions?q=members&projection=(elements*(title,company,startDate,endDate))")
            positions_response.raise_for_status()
            positions = positions_response.json()
            
            # Get skills
            skills_response = await client.get("/skills?q=members&projection=(elements*(name,proficiency))")
            skills_response.raise_for_status()
            skills = skills_response.json()
            
            # Get industry
            industry_response = await client.get("/industry?q=members&projection=(elements*(name))")
            industry_response.raise_for_status()
            industry = industry_response.json()
            
            return {
                'profile': profile,
                'email': email_data,
                'positions': positions,
                'skills': skills,
                'industry': industry
            }
            
        except Exception as e:
            logger.error(f"Error getting profile data: {str(e)}")
            return None
            
    async def _extract_professional_traits(self, client: httpx.AsyncClient) -> Dict[str, Any]:
        """Extract professional traits from LinkedIn profile data."""
        try:
            # Get profile data
            profile_data = await self.get_profile_data(client)
            if not profile_data:
                return {}
                
            # Extract job titles from positions
            job_titles = []
            experience_years = 0
            if positions := profile_data.get('positions', {}).get('elements', []):
                for position in positions:
                    if title := position.get('title'):
                        job_titles.append(title)
                    # Calculate years of experience
                    if 'startDate' in position:
                        start_date = datetime(
                            year=position['startDate'].get('year', 1970),
                            month=position['startDate'].get('month', 1),
                            day=1
                        )
                        end_date = None
                        if 'endDate' in position:
                            end_date = datetime(
                                year=position['endDate'].get('year', 1970),
                                month=position['endDate'].get('month', 1),
                                day=1
                            )
                        else:
                            end_date = datetime.utcnow()
                        experience_years += (end_date - start_date).days / 365
                        
            # Extract industries
            industries = []
            if industry_data := profile_data.get('industry', {}).get('elements', []):
                for industry in industry_data:
                    if name := industry.get('name'):
                        industries.append(name)
                        
            # Extract skills
            skills = []
            if skills_data := profile_data.get('skills', {}).get('elements', []):
                for skill in skills_data:
                    if name := skill.get('name'):
                        skills.append(name)
            
            return {
                "job_titles": job_titles,
                "industries": industries,
                "skills": skills,
                "experience_years": round(experience_years, 1),
                "last_updated": datetime.utcnow().isoformat(),
                "next_update": (datetime.utcnow() + timedelta(days=7)).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error extracting professional traits: {str(e)}")
            return {}
            
    async def _get_valid_token(self, user_id: str) -> Optional[OAuthToken]:
        """Get a valid OAuth token for the user, refreshing if necessary."""
        try:
            result = await self.db.execute(
                select(OAuthToken).filter_by(
                    user_id=user_id,
                    provider="linkedin"
                )
            )
            token = result.scalar_one_or_none()
            
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