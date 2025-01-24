from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from sqlalchemy.orm import Session
from app.models.oauth import OAuthToken
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

class SpotifyService:
    """Service for handling Spotify OAuth and trait extraction."""
    
    def __init__(self, db: Session):
        self.db = db
        self._client = None
        self.scopes = [
            'user-read-private',
            'user-read-email',
            'user-top-read',
            'user-read-recently-played',
            'playlist-read-private',
            'playlist-read-collaborative',
            'user-library-read'
        ]
    
    async def get_client(self, user_id: str) -> Optional[spotipy.Spotify]:
        """Get an authenticated Spotify client for a user."""
        try:
            print(f"\nGetting valid token for user {user_id}")
            token = await self._get_valid_token(user_id)
            if not token:
                print(f"No valid token found for user {user_id}")
                return None
                
            print("\nToken found:")
            print(f"  ID: {token.id}")
            print(f"  User ID: {token.user_id}")
            print(f"  Provider: {token.provider}")
            print(f"  Expires at: {token.expires_at}")
            print(f"  Access token length: {len(token.access_token) if token.access_token else 0}")
            print(f"  Refresh token length: {len(token.refresh_token) if token.refresh_token else 0}")
            print(f"  Client ID: {'*' * 10}{token.client_id[-5:] if token.client_id else 'None'}")
            print(f"  Redirect URI: {token.redirect_uri}")
            print(f"  Scope: {token.scope}")
                
            print("\nCreating SpotifyOAuth manager...")
            auth_manager = spotipy.oauth2.SpotifyOAuth(
                client_id=token.client_id,
                client_secret=token.client_secret,
                redirect_uri=token.redirect_uri,
                scope=' '.join(self.scopes),
                cache_handler=None  # We manage our own token storage
            )
            print("SpotifyOAuth manager created successfully")
            
            # Create client with token and increased timeout
            try:
                print("\nCreating Spotify client...")
                client = spotipy.Spotify(
                    auth=token.access_token,
                    auth_manager=auth_manager,
                    requests_timeout=30  # Increase timeout to 30 seconds
                )
                print("Spotify client created successfully")
                
                # Test the client
                try:
                    print("\nTesting client with current_user() call...")
                    user = client.current_user()
                    print(f"Successfully authenticated as Spotify user: {user['id']}")
                    return client
                except Exception as e:
                    print(f"\nError testing Spotify client: {str(e)}")
                    print(f"Error type: {type(e).__name__}")
                    if hasattr(e, 'response') and hasattr(e.response, 'text'):
                        print(f"Response text: {e.response.text}")
                    return None
                    
            except Exception as e:
                print(f"\nError creating Spotify client: {str(e)}")
                print(f"Error type: {type(e).__name__}")
                if hasattr(e, '__traceback__'):
                    import traceback
                    print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                return None
            
        except Exception as e:
            print(f"\nError getting Spotify client for user {user_id}: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            if hasattr(e, '__traceback__'):
                import traceback
                print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            return None
    
    async def extract_user_traits(self, user_id: str) -> Dict[str, Any]:
        """Extract user traits from Spotify data."""
        try:
            client = await self.get_client(user_id)
            if not client:
                logger.error(f"Could not get Spotify client for user {user_id}")
                return {}
                
            traits = {
                'music': await self._extract_music_traits(client),
                'social': await self._extract_social_traits(client),
                'behavior': await self._extract_behavior_traits(client),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Successfully extracted Spotify traits for user {user_id}")
            return traits
            
        except Exception as e:
            logger.error(f"Error extracting Spotify traits for user {user_id}: {str(e)}")
            return {}
    
    async def _extract_music_traits(self, client: spotipy.Spotify) -> Dict[str, Any]:
        """Extract music-related traits."""
        try:
            # Get top tracks for different time ranges
            ranges = ['short_term', 'medium_term', 'long_term']
            top_tracks = {}
            top_artists = {}
            top_genres = {}
            
            for time_range in ranges:
                # Get top tracks
                tracks = client.current_user_top_tracks(
                    limit=50,
                    offset=0,
                    time_range=time_range
                )
                top_tracks[time_range] = [
                    {
                        'id': track['id'],
                        'name': track['name'],
                        'artists': [artist['name'] for artist in track['artists']],
                        'popularity': track['popularity']
                    }
                    for track in tracks['items']
                ]
                
                # Get top artists
                artists = client.current_user_top_artists(
                    limit=50,
                    offset=0,
                    time_range=time_range
                )
                top_artists[time_range] = [
                    {
                        'id': artist['id'],
                        'name': artist['name'],
                        'genres': artist['genres'],
                        'popularity': artist['popularity']
                    }
                    for artist in artists['items']
                ]
                
                # Aggregate genres
                all_genres = []
                for artist in artists['items']:
                    all_genres.extend(artist['genres'])
                
                from collections import Counter
                genre_counts = Counter(all_genres)
                top_genres[time_range] = [
                    {'genre': genre, 'count': count}
                    for genre, count in genre_counts.most_common(20)
                ]
            
            return {
                'top_tracks': top_tracks,
                'top_artists': top_artists,
                'top_genres': top_genres
            }
            
        except Exception as e:
            logger.error(f"Error extracting music traits: {str(e)}")
            return {}
    
    async def _extract_social_traits(self, client: spotipy.Spotify) -> Dict[str, Any]:
        """Extract social-related traits."""
        try:
            # Get user's playlists
            playlists = client.current_user_playlists()
            
            # Analyze playlist behavior
            playlist_analysis = {
                'total_playlists': playlists['total'],
                'collaborative_playlists': 0,
                'public_playlists': 0,
                'avg_tracks_per_playlist': 0,
                'total_tracks': 0
            }
            
            for playlist in playlists['items']:
                if playlist['collaborative']:
                    playlist_analysis['collaborative_playlists'] += 1
                if playlist['public']:
                    playlist_analysis['public_playlists'] += 1
                    
                playlist_analysis['total_tracks'] += playlist['tracks']['total']
            
            if playlists['total'] > 0:
                playlist_analysis['avg_tracks_per_playlist'] = (
                    playlist_analysis['total_tracks'] / playlists['total']
                )
            
            return {
                'playlist_behavior': playlist_analysis
            }
            
        except Exception as e:
            logger.error(f"Error extracting social traits: {str(e)}")
            return {}
    
    async def _extract_behavior_traits(self, client: spotipy.Spotify) -> Dict[str, Any]:
        """Extract behavior-related traits."""
        try:
            # Get recently played tracks
            recent_tracks = client.current_user_recently_played(limit=50)
            
            # Analyze listening patterns
            from collections import defaultdict
            import pytz
            from datetime import timezone
            
            time_patterns = defaultdict(int)
            consecutive_plays = defaultdict(int)
            last_artist = None
            
            for item in recent_tracks['items']:
                # Convert timestamp to hour
                played_at = datetime.fromisoformat(
                    item['played_at'].replace('Z', '+00:00')
                )
                hour = played_at.astimezone(timezone.utc).hour
                time_patterns[hour] += 1
                
                # Track consecutive plays
                current_artist = item['track']['artists'][0]['id']
                if last_artist == current_artist:
                    consecutive_plays[current_artist] += 1
                last_artist = current_artist
            
            # Determine primary listening times
            active_hours = sorted(
                time_patterns.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            # Determine artist loyalty
            loyal_artists = sorted(
                consecutive_plays.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            return {
                'listening_patterns': {
                    'peak_hours': [hour for hour, _ in active_hours],
                    'loyal_artists': [artist for artist, _ in loyal_artists]
                }
            }
            
        except Exception as e:
            logger.error(f"Error extracting behavior traits: {str(e)}")
            return {}
    
    async def _get_valid_token(self, user_id: str) -> Optional[OAuthToken]:
        """Get a valid OAuth token for the user."""
        try:
            print(f"\nQuerying database for token (user_id={user_id}, provider=spotify)")
            token = self.db.query(OAuthToken).filter_by(
                user_id=user_id,
                provider="spotify"
            ).first()
            
            if not token:
                print(f"No token found for user {user_id}")
                return None
                
            print("\nToken found in database:")
            print(f"  ID: {token.id}")
            print(f"  User ID: {token.user_id}")
            print(f"  Provider: {token.provider}")
            print(f"  Expires at: {token.expires_at}")
            print(f"  Access token length: {len(token.access_token) if token.access_token else 0}")
            print(f"  Refresh token length: {len(token.refresh_token) if token.refresh_token else 0}")
            print(f"  Client ID: {'*' * 10}{token.client_id[-5:] if token.client_id else 'None'}")
            print(f"  Scope: {token.scope}")
                
            # Check if token needs refresh
            now = datetime.utcnow()
            # Convert expires_at to naive datetime for comparison
            expires_at = token.expires_at.replace(tzinfo=None) if token.expires_at.tzinfo else token.expires_at
            
            print(f"\nComparing times:")
            print(f"  Current time (UTC): {now}")
            print(f"  Expires at (naive): {expires_at}")
            
            if now >= expires_at:
                print(f"\nToken {token.id} has expired:")
                print(f"  Expires at: {expires_at}")
                print(f"  Current time: {now}")
                print("Attempting to refresh token...")
                
                token = await self._refresh_token(token)
                if not token:
                    print("Token refresh failed")
                    return None
                print("Token refreshed successfully")
            else:
                print(f"\nToken is still valid (expires in {expires_at - now})")
            
            return token
            
        except Exception as e:
            print(f"\nError getting valid token for user {user_id}: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            if hasattr(e, '__traceback__'):
                import traceback
                print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            return None
    
    async def _refresh_token(self, token: OAuthToken) -> Optional[OAuthToken]:
        """Refresh an expired OAuth token."""
        try:
            logger.info(f"Refreshing token {token.id} for user {token.user_id}")
            logger.info("Token details before refresh:")
            logger.info(f"  client_id: {'*' * 10}{token.client_id[-5:] if token.client_id else 'None'}")
            logger.info(f"  client_secret: {'*' * 10}{token.client_secret[-5:] if token.client_secret else 'None'}")
            logger.info(f"  redirect_uri: {token.redirect_uri}")
            logger.info(f"  refresh_token length: {len(token.refresh_token) if token.refresh_token else 0}")
            logger.info(f"  scope: {token.scope}")
            
            auth = spotipy.oauth2.SpotifyOAuth(
                client_id=token.client_id,
                client_secret=token.client_secret,
                redirect_uri=token.redirect_uri,
                scope=' '.join(self.scopes)
            )
            
            logger.info("Created SpotifyOAuth manager for refresh")
            
            try:
                # Get new token
                new_token_info = auth.refresh_access_token(token.refresh_token)
                logger.info("Successfully got new token from Spotify")
                logger.info(f"New token info keys: {list(new_token_info.keys())}")
                
                # Update token in database
                token.access_token = new_token_info['access_token']
                if new_token_info.get('refresh_token'):
                    logger.info("Received new refresh token")
                    token.refresh_token = new_token_info['refresh_token']
                token.expires_at = datetime.utcnow() + timedelta(
                    seconds=new_token_info['expires_in']
                )
                
                self.db.commit()
                logger.info(f"Successfully refreshed token for user {token.user_id}")
                logger.info("Updated token details:")
                logger.info(f"  expires_at: {token.expires_at}")
                logger.info(f"  access_token length: {len(token.access_token) if token.access_token else 0}")
                logger.info(f"  refresh_token length: {len(token.refresh_token) if token.refresh_token else 0}")
                
                return token
                
            except Exception as e:
                logger.error(f"Error refreshing token with Spotify: {str(e)}")
                logger.error(f"Error type: {type(e).__name__}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    logger.error(f"Response text: {e.response.text}")
                return None
            
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            return None 