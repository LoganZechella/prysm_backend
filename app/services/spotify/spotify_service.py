"""Service for interacting with Spotify API."""
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import asyncio
from functools import partial
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging
import pytz

from app.models.oauth import OAuthToken
from app.core.config import settings

logger = logging.getLogger(__name__)

class SpotifyService:
    """Service for interacting with Spotify API."""

    def __init__(self, db: AsyncSession):
        """Initialize the Spotify service."""
        self.db = db
        self._client = None
        self._oauth = None
        self.scopes = [
            'user_read_private',
            'user_read_email',
            'user_top_read',
            'user_read_recently_played',
            'playlist_read_private',
            'playlist_read_collaborative',
            'user_library_read',
            'user_follow_read',
            'user_follow_modify'
        ]

    async def _run_sync(self, func, *args, **kwargs):
        """Run a synchronous function in an async context."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, partial(func, *args, **kwargs))

    async def get_client(self, user_id: str) -> Optional[spotipy.Spotify]:
        """Get an authenticated Spotify client for the user."""
        try:
            # Get valid token
            token = await self._get_valid_token(user_id)
            if not token:
                logger.error(f"No valid token found for user {user_id}")
                return None

            # Create OAuth manager if needed
            if not self._oauth:
                self._oauth = SpotifyOAuth(
                    client_id=token.client_id,
                    client_secret=settings.SPOTIFY_CLIENT_SECRET,
                    redirect_uri=token.redirect_uri,
                    scope=token.scope
                )

            # Create client with token
            self._client = spotipy.Spotify(
                auth=token.access_token,
                auth_manager=self._oauth
            )

            # Test the client
            try:
                user = await self._run_sync(self._client.current_user)
                logger.info(f"Successfully authenticated as Spotify user: {user['id']}")
                return self._client
            except Exception as e:
                logger.error(f"Error testing Spotify client: {str(e)}")
                return None

        except Exception as e:
            logger.error(f"Error getting Spotify client: {str(e)}")
            return None

    async def get_music_traits(self, user_id: str) -> Dict[str, Any]:
        """Get music-related traits for a user."""
        try:
            client = await self.get_client(user_id)
            if not client:
                return {}

            # Get top tracks for different time ranges
            top_tracks_short = await self._run_sync(client.current_user_top_tracks, limit=50, time_range="short_term")
            top_tracks_medium = await self._run_sync(client.current_user_top_tracks, limit=50, time_range="medium_term")
            top_tracks_long = await self._run_sync(client.current_user_top_tracks, limit=50, time_range="long_term")

            # Get top artists for different time ranges
            top_artists_short = await self._run_sync(client.current_user_top_artists, limit=50, time_range="short_term")
            top_artists_medium = await self._run_sync(client.current_user_top_artists, limit=50, time_range="medium_term")
            top_artists_long = await self._run_sync(client.current_user_top_artists, limit=50, time_range="long_term")

            return {
                "top_tracks": {
                    "short_term": top_tracks_short["items"] if top_tracks_short else [],
                    "medium_term": top_tracks_medium["items"] if top_tracks_medium else [],
                    "long_term": top_tracks_long["items"] if top_tracks_long else []
                },
                "top_artists": {
                    "short_term": top_artists_short["items"] if top_artists_short else [],
                    "medium_term": top_artists_medium["items"] if top_artists_medium else [],
                    "long_term": top_artists_long["items"] if top_artists_long else []
                }
            }

        except Exception as e:
            logger.error(f"Error getting music traits: {str(e)}")
            return {}

    async def get_social_traits(self, user_id: str) -> Dict[str, Any]:
        """Get social-related traits for a user."""
        try:
            client = await self.get_client(user_id)
            if not client:
                return {}

            # Get user's playlists
            playlists = await self._run_sync(client.current_user_playlists)
            
            # Get followed artists
            following = await self._run_sync(client.current_user_followed_artists)

            return {
                "playlists": playlists["items"] if playlists else [],
                "following": following["artists"]["items"] if following and "artists" in following else []
            }

        except Exception as e:
            logger.error(f"Error getting social traits: {str(e)}")
            return {}

    async def get_behavior_traits(self, user_id: str) -> Dict[str, Any]:
        """Get behavior-related traits for a user."""
        try:
            client = await self.get_client(user_id)
            if not client:
                return {}

            # Get recently played tracks
            recently_played = await self._run_sync(client.current_user_recently_played, limit=50)

            # Get saved tracks
            saved_tracks = await self._run_sync(client.current_user_saved_tracks, limit=50)

            return {
                "recently_played": recently_played["items"] if recently_played else [],
                "saved_tracks": saved_tracks["items"] if saved_tracks else []
            }

        except Exception as e:
            logger.error(f"Error getting behavior traits: {str(e)}")
            return {}

    async def _get_valid_token(self, user_id: str) -> Optional[OAuthToken]:
        """Get a valid OAuth token for the user."""
        try:
            # Query for token
            logger.info(f"\nQuerying database for token (user_id={user_id}, provider=spotify)")
            stmt = select(OAuthToken).where(
                OAuthToken.user_id == user_id,
                OAuthToken.provider == "spotify"
            )
            result = await self.db.execute(stmt)
            token = result.scalar_one_or_none()

            if not token:
                logger.error("No token found in database")
                return None

            logger.info("\nToken found in database:")
            logger.info(f"  ID: {token.id}")
            logger.info(f"  User ID: {token.user_id}")
            logger.info(f"  Provider: {token.provider}")
            logger.info(f"  Expires at: {token.expires_at}")
            logger.info(f"  Access token length: {len(token.access_token)}")
            logger.info(f"  Refresh token length: {len(token.refresh_token)}")
            logger.info(f"  Client ID: {'*' * 10}{token.client_id[-5:]}")
            logger.info(f"  Scope: {token.scope}")

            # Check if token needs refresh
            now = datetime.now(pytz.UTC)
            expires_at = token.expires_at.astimezone(pytz.UTC) if token.expires_at.tzinfo else pytz.UTC.localize(token.expires_at)

            logger.info("\nComparing times:")
            logger.info(f"  Current time (UTC): {now}")
            logger.info(f"  Expires at (UTC): {expires_at}")

            if expires_at > now:
                time_left = expires_at - now
                logger.info(f"\nToken is still valid (expires in {time_left})")
                return token

            logger.info("\nToken expired, refreshing...")

            # Create OAuth manager for refresh
            oauth = SpotifyOAuth(
                client_id=token.client_id,
                client_secret=settings.SPOTIFY_CLIENT_SECRET,
                redirect_uri=token.redirect_uri,
                scope=token.scope
            )

            # Refresh token
            refresh_token = token.refresh_token
            new_token = await self._run_sync(oauth.refresh_access_token, refresh_token)

            # Update token in database
            token.access_token = new_token['access_token']
            token.refresh_token = new_token.get('refresh_token', token.refresh_token)
            token.expires_at = pytz.UTC.localize(datetime.utcnow() + timedelta(seconds=new_token['expires_in']))

            await self.db.commit()
            logger.info("Token refreshed successfully")

            return token

        except Exception as e:
            logger.error(f"Error getting/refreshing token: {str(e)}")
            await self.db.rollback()
            return None

    async def get_top_tracks(self, limit: int = 50, time_range: str = "medium_term") -> Dict[str, Any]:
        """Get user's top tracks."""
        try:
            if not self._client:
                logger.error("No Spotify client available")
                return {}
            
            results = self._client.current_user_top_tracks(
                limit=limit,
                offset=0,
                time_range=time_range
            )
            return results
        except Exception as e:
            logger.error(f"Error getting top tracks: {str(e)}")
            return {}
    
    async def get_top_artists(self, limit: int = 50, time_range: str = "medium_term") -> Dict[str, Any]:
        """Get user's top artists."""
        try:
            if not self._client:
                logger.error("No Spotify client available")
                return {}
            
            results = self._client.current_user_top_artists(
                limit=limit,
                offset=0,
                time_range=time_range
            )
            return results
        except Exception as e:
            logger.error(f"Error getting top artists: {str(e)}")
            return {}
    
    async def get_user_playlists(self, limit: int = 50) -> Dict[str, Any]:
        """Get user's playlists."""
        try:
            if not self._client:
                logger.error("No Spotify client available")
                return {}
            
            results = self._client.current_user_playlists(limit=limit)
            return results
        except Exception as e:
            logger.error(f"Error getting user playlists: {str(e)}")
            return {}
    
    async def get_followed_artists(self, limit: int = 20) -> Dict[str, Any]:
        """Get user's followed artists."""
        try:
            if not self._client:
                logger.error("No Spotify client available")
                return {}
            
            # Get followed artists using the Spotipy library method
            try:
                results = self._client.current_user_following('artist')
                if not results:
                    return {'artists': {'items': [], 'total': 0}}
                
                return {'artists': {'items': results, 'total': len(results)}}
            except Exception as e:
                logger.error(f"Error getting followed artists: {str(e)}")
                return {'artists': {'items': [], 'total': 0}}
            
        except Exception as e:
            logger.error(f"Error getting followed artists: {str(e)}")
            return {'artists': {'items': [], 'total': 0}}
    
    async def get_recently_played(self, limit: int = 50) -> Dict[str, Any]:
        """Get user's recently played tracks."""
        try:
            if not self._client:
                logger.error("No Spotify client available")
                return {}
            
            results = self._client.current_user_recently_played(limit=limit)
            return results
        except Exception as e:
            logger.error(f"Error getting recently played tracks: {str(e)}")
            return {}
    
    async def close(self):
        """Close the database connection."""
        await self.db.close() 