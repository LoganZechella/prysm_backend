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
                
            traits = await self._extract_professional_traits(client)
            await self._store_profile_data(user_id, traits)
            return traits
            
        except Exception as e:
            logger.error(f"Error getting professional traits: {str(e)}")
            return {}
            
    async def _extract_professional_traits(self, client) -> Dict[str, Any]:
        """Extract professional traits from Google profile data."""
        try:
            # Get profile data with valid fields
            profile = client.people().get(
                resourceName='people/me',
                personFields='names,emailAddresses,organizations,occupations,locations,interests,skills,urls,birthdays,addresses,phoneNumbers,coverPhotos,genders,biographies,nicknames,photos'
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
            
            # Extract birthdays
            traits["birthdays"] = profile.get('birthdays', [])

            # Extract addresses
            traits["addresses"] = profile.get('addresses', [])

            # Extract phone numbers
            traits["phoneNumbers"] = profile.get('phoneNumbers', [])

            # Extract cover photos
            traits["coverPhotos"] = profile.get('coverPhotos', [])

            # Extract genders
            traits["genders"] = profile.get('genders', [])

            # Extract biographies
            traits["biographies"] = profile.get('biographies', [])

            # Extract nicknames
            traits["nicknames"] = profile.get('nicknames', [])

            # Extract photos
            traits["photos"] = profile.get('photos', [])
            
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

    async def _store_profile_data(self, user_id: str, profile_data: Dict[str, Any]) -> None:
        """Store Google profile data in the database with merging and version history."""
        try:
            from sqlalchemy import select
            from app.models.google_profile import GoogleProfile
            from datetime import datetime

            result = await self.db.execute(select(GoogleProfile).filter_by(user_id=user_id))
            existing_profile = result.scalar_one_or_none()

            current_ts = datetime.utcnow().isoformat()

            def merge_data(existing: dict, new: dict) -> dict:
                merged = {} if not existing else existing.copy()

                for field, new_value in new.items():
                    if field in merged:
                        old_entry = merged.get(field)
                        if isinstance(old_entry, dict) and 'value' in old_entry:
                            if old_entry['value'] != new_value:
                                prev_key = f'previous_{field}'
                                if prev_key not in merged:
                                    merged[prev_key] = []
                                merged[prev_key].append(old_entry)
                                merged[field] = {"value": new_value, "updated_at": current_ts}
                            # else, value is the same, do nothing
                        else:
                            merged[field] = {"value": new_value, "updated_at": current_ts}
                    else:
                        merged[field] = {"value": new_value, "updated_at": current_ts}
                return merged

            if existing_profile:
                updated_profile = merge_data(existing_profile.profile_data or {}, profile_data)
                existing_profile.profile_data = updated_profile
            else:
                new_profile_struct = {}
                for field, value in profile_data.items():
                    new_profile_struct[field] = {"value": value, "updated_at": current_ts}
                new_profile = GoogleProfile(user_id=user_id, profile_data=new_profile_struct)
                self.db.add(new_profile)

            await self.db.commit()
        except Exception as e:
            logger.error(f"Error storing google profile data for user {user_id}: {str(e)}") 