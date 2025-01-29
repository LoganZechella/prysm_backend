import os
import logging
import aiohttp
from typing import Dict, Any
from base64 import b64encode

logger = logging.getLogger(__name__)

class SpotifyClient:
    """Client for interacting with Spotify Web API."""
    
    def __init__(self):
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
        
        if not all([self.client_id, self.client_secret, self.redirect_uri]):
            raise ValueError("Missing required Spotify credentials")
            
        # Create Basic auth header
        credentials = f"{self.client_id}:{self.client_secret}"
        self.auth_header = b64encode(credentials.encode()).decode()
        
    async def exchange_code(self, code: str) -> Dict[str, Any]:
        """Exchange authorization code for access token."""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://accounts.spotify.com/api/token",
                headers={
                    "Authorization": f"Basic {self.auth_header}",
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.redirect_uri
                }
            ) as response:
                if response.status != 200:
                    error_data = await response.json()
                    logger.error(f"Spotify token exchange failed: {error_data}")
                    raise ValueError(f"Failed to exchange code: {error_data.get('error_description', 'Unknown error')}")
                    
                return await response.json()
                
    async def refresh_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh an expired access token."""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://accounts.spotify.com/api/token",
                headers={
                    "Authorization": f"Basic {self.auth_header}",
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token
                }
            ) as response:
                if response.status != 200:
                    error_data = await response.json()
                    logger.error(f"Spotify token refresh failed: {error_data}")
                    raise ValueError(f"Failed to refresh token: {error_data.get('error_description', 'Unknown error')}")
                    
                return await response.json() 