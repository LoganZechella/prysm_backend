import os
import json
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_spotify_client():
    """Initialize and return a Spotify client."""
    client_credentials_manager = SpotifyClientCredentials(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET')
    )
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def fetch_trending_tracks(client, limit=50):
    """Fetch trending tracks from Spotify."""
    # Get featured playlists
    playlists = client.featured_playlists(limit=5)['playlists']['items']
    
    tracks = []
    for playlist in playlists:
        # Get tracks from each playlist
        results = client.playlist_tracks(playlist['id'], limit=limit//5)
        tracks.extend([item['track'] for item in results['items'] if item['track']])
    
    return tracks

def upload_to_gcs(data, bucket_name, blob_path):
    """Upload data to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Convert data to JSON string
    json_data = json.dumps(data)
    
    # Upload to GCS
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 