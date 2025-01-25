"""Script to create Spotify OAuth token in the database."""
from datetime import datetime, timedelta
from app.database import SessionLocal
from app.models.oauth import OAuthToken
from app.core.config import settings

def create_spotify_token():
    """Create Spotify OAuth token in the database."""
    db = SessionLocal()
    try:
        # Create token with real credentials
        token = OAuthToken(
            user_id="loganzechella",
            provider="spotify",
            access_token=settings.SPOTIFY_TEST_ACCESS_TOKEN,
            refresh_token=settings.SPOTIFY_TEST_REFRESH_TOKEN,
            expires_at=datetime.utcnow() + timedelta(hours=1),
            client_id=settings.SPOTIFY_CLIENT_ID,
            client_secret=settings.SPOTIFY_CLIENT_SECRET,
            redirect_uri=settings.SPOTIFY_REDIRECT_URI,
            scope=" ".join([
                'user_read_private',
                'user_read_email',
                'user_top_read',
                'user_read_recently_played',
                'playlist_read_private',
                'playlist_read_collaborative',
                'user_library_read',
                'user_follow_read',
                'user_follow_modify'
            ])
        )
        
        # Add to database
        db.add(token)
        db.commit()
        db.refresh(token)
        
        print(f"Successfully created token for user {token.user_id}")
        print(f"Token details:")
        print(f"  ID: {token.id}")
        print(f"  Provider: {token.provider}")
        print(f"  Expires at: {token.expires_at}")
        print(f"  Scope: {token.scope}")
        
    except Exception as e:
        print(f"Error creating token: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            print(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
    finally:
        db.close()

if __name__ == "__main__":
    create_spotify_token() 