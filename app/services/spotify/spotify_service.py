"""Service for handling Spotify OAuth and trait extraction."""
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
                scope=token.scope,  # Use the token's scope
                cache_handler=None  # We manage our own token storage
            )
            print("SpotifyOAuth manager created successfully")
            
            # Create client with token and increased timeout
            try:
                print("\nCreating Spotify client...")
                client = spotipy.Spotify(
                    auth=token.access_token,
                    auth_manager=auth_manager,
                    requests_timeout=30,  # Increase timeout to 30 seconds
                    requests_session=None  # Don't reuse sessions
                )
                print("Spotify client created successfully")
                
                # Test the client
                try:
                    print("\nTesting client with current_user() call...")
                    user = client.current_user()
                    print(f"Successfully authenticated as Spotify user: {user['id']}")
                    self._client = client  # Store the client
                    return self._client
                except spotipy.exceptions.SpotifyException as e:
                    if e.http_status == 401:
                        print("\nGot 401 error, attempting to refresh token...")
                        token = await self._refresh_token(token)
                        if token:
                            print("\nToken refreshed, retrying with new token...")
                            client = spotipy.Spotify(
                                auth=token.access_token,
                                auth_manager=auth_manager,
                                requests_timeout=30,
                                requests_session=None
                            )
                            user = client.current_user()
                            print(f"Successfully authenticated as Spotify user: {user['id']}")
                            self._client = client
                            return self._client
                    print(f"\nError testing Spotify client: {str(e)}")
                    print(f"Error type: {type(e).__name__}")
                    if hasattr(e, 'response') and hasattr(e.response, 'text'):
                        print(f"Response text: {e.response.text}")
                    self._client = None
                    return None
                    
            except Exception as e:
                print(f"\nError creating Spotify client: {str(e)}")
                print(f"Error type: {type(e).__name__}")
                if hasattr(e, '__traceback__'):
                    import traceback
                    print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                self._client = None
                return None
                
        except Exception as e:
            print(f"\nError getting Spotify client for user {user_id}: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            if hasattr(e, '__traceback__'):
                import traceback
                print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            self._client = None
            return None
    
    async def extract_user_traits(self, user_id: str) -> Dict[str, Any]:
        """Extract user traits from Spotify data."""
        try:
            client = await self.get_client(user_id)
            if not client:
                logger.error(f"Could not get Spotify client for user {user_id}")
                return {}
                
            # Get music traits
            music_traits = {}
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
                
                music_traits = {
                    'top_tracks': top_tracks,
                    'top_artists': top_artists,
                    'top_genres': top_genres
                }
            except Exception as e:
                logger.error(f"Error extracting music traits: {str(e)}")
                music_traits = {}
            
            # Get social traits
            social_traits = {}
            try:
                # Get user's playlists
                playlists = client.current_user_playlists()
                
                # Get followed artists
                followed = client.current_user_followed_artists()
                
                # Analyze playlist behavior
                playlist_analysis = {
                    'total_playlists': playlists['total'] if playlists else 0,
                    'collaborative_playlists': 0,
                    'public_playlists': 0,
                    'avg_tracks_per_playlist': 0,
                    'total_tracks': 0,
                    'total_following': 0  # Initialize to 0
                }
                
                # Process playlists
                if playlists and 'items' in playlists:
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
                
                # Process followed artists
                if followed and 'artists' in followed:
                    playlist_analysis['total_following'] = followed['artists']['total']
                
                social_traits = {
                    'playlist_behavior': playlist_analysis
                }
            except Exception as e:
                logger.error(f"Error extracting social traits: {str(e)}")
                social_traits = {}
            
            # Get behavior traits
            behavior_traits = {}
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
                
                # Convert defaultdict to regular dict for JSON serialization
                behavior_traits = {
                    'listening_patterns': {
                        'time_of_day': dict(time_patterns),
                        'consecutive_plays': dict(consecutive_plays)
                    }
                }
            except Exception as e:
                logger.error(f"Error extracting behavior traits: {str(e)}")
                behavior_traits = {}
            
            traits = {
                'music': music_traits,
                'social': social_traits,
                'behavior': behavior_traits,
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
                tracks = await self.get_top_tracks(limit=50, time_range=time_range)
                top_tracks[time_range] = [
                    {
                        'id': track['id'],
                        'name': track['name'],
                        'artists': [artist['name'] for artist in track['artists']],
                        'popularity': track['popularity']
                    }
                    for track in tracks['items']
                ] if tracks else []
                
                # Get top artists
                artists = await self.get_top_artists(limit=50, time_range=time_range)
                top_artists[time_range] = [
                    {
                        'id': artist['id'],
                        'name': artist['name'],
                        'genres': artist['genres'],
                        'popularity': artist['popularity']
                    }
                    for artist in artists['items']
                ] if artists else []
                
                # Aggregate genres
                all_genres = []
                if artists:
                    for artist in artists['items']:
                        all_genres.extend(artist['genres'])
                
                    from collections import Counter
                    genre_counts = Counter(all_genres)
                    top_genres[time_range] = [
                        {'genre': genre, 'count': count}
                        for genre, count in genre_counts.most_common(20)
                    ]
                else:
                    top_genres[time_range] = []
            
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
            # Get user's playlists and followed artists
            playlists = await self.get_user_playlists()
            followed = await self.get_followed_artists()
            
            # Analyze playlist behavior
            playlist_analysis = {
                'total_playlists': playlists['total'] if playlists else 0,
                'collaborative_playlists': 0,
                'public_playlists': 0,
                'avg_tracks_per_playlist': 0,
                'total_tracks': 0,
                'total_following': followed['artists']['total'] if followed and 'artists' in followed else 0
            }
            
            if playlists and 'items' in playlists:
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
            recent_tracks = await self.get_recently_played(limit=50)
            
            # Analyze listening patterns
            from collections import defaultdict
            import pytz
            from datetime import timezone
            
            time_patterns = defaultdict(int)
            consecutive_plays = defaultdict(int)
            last_artist = None
            
            if recent_tracks and 'items' in recent_tracks:
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
            
            # Convert defaultdict to regular dict for JSON serialization
            return {
                'listening_patterns': {
                    'time_of_day': dict(time_patterns),
                    'consecutive_plays': dict(consecutive_plays)
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