import pytest
from datetime import datetime, timedelta
import os
from sqlalchemy.orm import Session
from app.models.oauth import OAuthToken
from app.services.spotify_service import SpotifyService
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

@pytest.mark.integration
class TestSpotifyIntegration:
    """Integration tests for Spotify service using real credentials."""
    
    @pytest.fixture(autouse=True)
    def check_env_vars(self):
        """Check that all required environment variables are set."""
        required_vars = [
            'SPOTIFY_CLIENT_ID',
            'SPOTIFY_CLIENT_SECRET',
            'SPOTIFY_REDIRECT_URI',
            'SPOTIFY_TEST_ACCESS_TOKEN',
            'SPOTIFY_TEST_REFRESH_TOKEN'
        ]
        
        # Log all environment variables for debugging
        logger.info("Current environment variables:")
        for var in required_vars:
            value = os.getenv(var)
            if not value:
                logger.error(f"{var} is not set!")
            else:
                masked_value = '*' * 10 + value[-5:] if value else 'Not set'
                logger.info(f"{var}: {masked_value}")
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            pytest.fail(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    @pytest.fixture
    def spotify_token(self, test_db: Session) -> OAuthToken:
        """Create a test OAuth token using real credentials."""
        print("\nCreating Spotify test token...")
        
        # Log all relevant environment variables
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
        access_token = os.getenv("SPOTIFY_TEST_ACCESS_TOKEN")
        refresh_token = os.getenv("SPOTIFY_TEST_REFRESH_TOKEN")
        
        print("\nToken creation parameters:")
        print(f"  client_id: {'*' * 10}{client_id[-5:] if client_id else 'None'}")
        print(f"  client_secret: {'*' * 10}{client_secret[-5:] if client_secret else 'None'}")
        print(f"  redirect_uri: {redirect_uri}")
        print(f"  access_token length: {len(access_token) if access_token else 0}")
        print(f"  refresh_token length: {len(refresh_token) if refresh_token else 0}")
        
        # Create token with detailed error handling
        try:
            token = OAuthToken(
                user_id="test_user",
                provider="spotify",
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=datetime.utcnow() + timedelta(hours=1),
                token_type="Bearer",
                scope=" ".join([
                    'user-read-private',
                    'user-read-email',
                    'user-top-read',
                    'user-read-recently-played',
                    'playlist-read-private',
                    'playlist-read-collaborative',
                    'user-library-read'
                ])
            )
            
            print("\nToken object created successfully")
            print("Token details:")
            print(f"  user_id: {token.user_id}")
            print(f"  provider: {token.provider}")
            print(f"  expires_at: {token.expires_at}")
            print(f"  access_token length: {len(token.access_token) if token.access_token else 0}")
            print(f"  refresh_token length: {len(token.refresh_token) if token.refresh_token else 0}")
            print(f"  client_id: {'*' * 10}{token.client_id[-5:] if token.client_id else 'None'}")
            print(f"  scope: {token.scope}")
            
            # Add to database with error handling
            try:
                test_db.add(token)
                test_db.commit()
                print("\nToken committed to database successfully")
            except Exception as e:
                print(f"\nError committing token to database: {str(e)}")
                print(f"Error type: {type(e).__name__}")
                test_db.rollback()
                raise
            
            # Verify token was saved
            saved_token = test_db.query(OAuthToken).filter_by(
                user_id="test_user",
                provider="spotify"
            ).first()
            
            if saved_token:
                print("\nToken verification after save:")
                print(f"  ID: {saved_token.id}")
                print(f"  User ID: {saved_token.user_id}")
                print(f"  Provider: {saved_token.provider}")
                print(f"  Expires at: {saved_token.expires_at}")
                print(f"  Access token length: {len(saved_token.access_token) if saved_token.access_token else 0}")
                print(f"  Refresh token length: {len(saved_token.refresh_token) if saved_token.refresh_token else 0}")
                print(f"  Client ID: {'*' * 10}{saved_token.client_id[-5:] if saved_token.client_id else 'None'}")
            else:
                print("\nERROR: Token was not found in database after save!")
                
            return token
            
        except Exception as e:
            print(f"\nError creating token object: {str(e)}")
            print(f"Error type: {type(e).__name__}")
            if hasattr(e, '__traceback__'):
                import traceback
                print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            raise
    
    @pytest.fixture
    def spotify_service(self, test_db: Session) -> SpotifyService:
        """Create a SpotifyService instance."""
        return SpotifyService(test_db)
    
    @pytest.mark.asyncio
    async def test_get_client(
        self,
        spotify_service: SpotifyService,
        spotify_token: OAuthToken
    ):
        """Test getting an authenticated Spotify client."""
        try:
            # Verify token exists before test
            token = spotify_service.db.query(OAuthToken).filter_by(
                user_id="test_user",
                provider="spotify"
            ).first()
            
            if not token:
                logger.error("No token found in database before test")
                pytest.fail("Token not found in database")
            
            logger.info("Token found in database:")
            logger.info(f"  ID: {token.id}")
            logger.info(f"  User ID: {token.user_id}")
            logger.info(f"  Provider: {token.provider}")
            logger.info(f"  Expires at: {token.expires_at}")
            logger.info(f"  Access token length: {len(token.access_token) if token.access_token else 0}")
            logger.info(f"  Refresh token length: {len(token.refresh_token) if token.refresh_token else 0}")
            logger.info(f"  Client ID: {'*' * 10}{token.client_id[-5:] if token.client_id else 'None'}")
            logger.info(f"  Redirect URI: {token.redirect_uri}")
            logger.info(f"  Scope: {token.scope}")
            
            # Attempt to get client
            logger.info("Attempting to get Spotify client...")
            client = await spotify_service.get_client("test_user")
            
            # Check client
            if client is None:
                logger.error("Failed to get Spotify client")
                pytest.fail("Failed to get Spotify client")
            
            logger.info("Successfully got Spotify client")
            
            # Test that we can make an API call
            logger.info("Testing API call with client...")
            try:
                user = client.current_user()
                logger.info(f"Successfully got current user: {user['id']}")
                assert user is not None, "Failed to get current user"
                assert 'id' in user, "User response missing 'id' field"
            except Exception as e:
                logger.error(f"Error making API call: {str(e)}")
                logger.error(f"Error type: {type(e).__name__}")
                if hasattr(e, 'response') and hasattr(e.response, 'text'):
                    logger.error(f"Response text: {e.response.text}")
                raise
                
        except Exception as e:
            logger.error(f"Test failed with error: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            raise
    
    @pytest.mark.asyncio
    async def test_extract_user_traits(
        self,
        spotify_service: SpotifyService,
        spotify_token: OAuthToken
    ):
        """Test extracting user traits from Spotify."""
        traits = await spotify_service.extract_user_traits("test_user")
        
        # Verify music traits
        assert 'music' in traits, "Missing 'music' in traits"
        assert 'top_tracks' in traits['music'], "Missing 'top_tracks' in music traits"
        assert 'top_artists' in traits['music'], "Missing 'top_artists' in music traits"
        assert 'top_genres' in traits['music'], "Missing 'top_genres' in music traits"
        
        # Verify time ranges
        for time_range in ['short_term', 'medium_term', 'long_term']:
            assert time_range in traits['music']['top_tracks'], f"Missing {time_range} in top_tracks"
            assert time_range in traits['music']['top_artists'], f"Missing {time_range} in top_artists"
            assert time_range in traits['music']['top_genres'], f"Missing {time_range} in top_genres"
        
        # Verify social traits
        assert 'social' in traits, "Missing 'social' in traits"
        assert 'playlist_behavior' in traits['social'], "Missing 'playlist_behavior' in social traits"
        playlist_behavior = traits['social']['playlist_behavior']
        assert 'total_playlists' in playlist_behavior, "Missing 'total_playlists' in playlist_behavior"
        assert 'collaborative_playlists' in playlist_behavior, "Missing 'collaborative_playlists' in playlist_behavior"
        assert 'public_playlists' in playlist_behavior, "Missing 'public_playlists' in playlist_behavior"
        
        # Verify behavior traits
        assert 'behavior' in traits, "Missing 'behavior' in traits"
        assert 'listening_patterns' in traits['behavior'], "Missing 'listening_patterns' in behavior traits"
        patterns = traits['behavior']['listening_patterns']
        assert 'peak_hours' in patterns, "Missing 'peak_hours' in listening_patterns"
        assert 'loyal_artists' in patterns, "Missing 'loyal_artists' in listening_patterns"
        
        # Verify timestamp
        assert 'timestamp' in traits, "Missing 'timestamp' in traits"
        timestamp = datetime.fromisoformat(traits['timestamp'])
        assert isinstance(timestamp, datetime), "Invalid timestamp format"
    
    @pytest.mark.asyncio
    async def test_token_refresh(
        self,
        spotify_service: SpotifyService,
        spotify_token: OAuthToken,
        test_db: Session
    ):
        """Test token refresh functionality."""
        # Set token to expired
        spotify_token.expires_at = datetime.utcnow() - timedelta(hours=1)
        test_db.commit()
        
        # Store original access token
        original_access_token = spotify_token.access_token
        
        # Verify token is expired
        token = test_db.query(OAuthToken).filter_by(
            user_id="test_user",
            provider="spotify"
        ).first()
        logger.info(f"Token before refresh: expires_at={token.expires_at}, access_token={'*' * 10}{token.access_token[-5:] if token.access_token else 'None'}")
        
        # Get client (should trigger refresh)
        client = await spotify_service.get_client("test_user")
        assert client is not None, "Failed to get Spotify client after token refresh"
        
        # Verify token was refreshed
        refreshed_token = test_db.query(OAuthToken).filter_by(
            user_id="test_user",
            provider="spotify"
        ).first()
        
        assert refreshed_token is not None, "Token not found after refresh"
        
        # Convert both datetimes to naive UTC for comparison
        now = datetime.utcnow()
        refreshed_expires = refreshed_token.expires_at.replace(tzinfo=None) if refreshed_token.expires_at.tzinfo else refreshed_token.expires_at
        
        assert refreshed_expires > now, "Token still expired after refresh"
        assert refreshed_token.access_token != original_access_token, "Token not refreshed"
        
        logger.info(f"Token after refresh: expires_at={refreshed_token.expires_at}, access_token={'*' * 10}{refreshed_token.access_token[-5:] if refreshed_token.access_token else 'None'}") 