"""Script to get test tokens from Spotify."""
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv

def get_spotify_tokens():
    """Get test tokens from Spotify."""
    # Load environment variables
    load_dotenv()
    
    # Set up Spotify OAuth using environment variables
    auth_manager = SpotifyOAuth(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET'),
        redirect_uri=os.getenv('SPOTIFY_REDIRECT_URI'),
        scope=' '.join([
            'user-read-private',
            'user-read-email',
            'user-top-read',
            'user-read-recently-played',
            'playlist-read-private',
            'playlist-read-collaborative',
            'user-library-read'
        ]),
        open_browser=False
    )
    
    # Get authorization URL
    auth_url = auth_manager.get_authorize_url()
    print("\nPlease visit this URL to authorize the application:")
    print(auth_url)
    
    print("\nAfter authorizing, you will be redirected to a URL.")
    print("Copy the entire URL from your browser's address bar after being redirected.")
    
    # Get the code from the redirect URL
    redirect_url = input("\nPaste the full redirect URL here: ")
    code = auth_manager.parse_response_code(redirect_url)
    
    # Get tokens
    token_info = auth_manager.get_access_token(code)
    
    print("\nAdd these environment variables to your .env file:")
    print(f"SPOTIFY_TEST_ACCESS_TOKEN={token_info['access_token']}")
    print(f"SPOTIFY_TEST_REFRESH_TOKEN={token_info['refresh_token']}")
    
    # Test the token
    spotify = spotipy.Spotify(auth_manager=auth_manager)
    user = spotify.current_user()
    print(f"\nSuccessfully authenticated as: {user['display_name']} ({user['id']})")

if __name__ == '__main__':
    get_spotify_tokens() 