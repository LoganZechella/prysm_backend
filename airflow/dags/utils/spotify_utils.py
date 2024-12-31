import os
import json
from typing import Dict, Any, List
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_spotify_client() -> spotipy.Spotify:
    """Initialize and return a Spotify client."""
    client_credentials_manager = SpotifyClientCredentials(
        client_id=os.getenv('SPOTIFY_CLIENT_ID'),
        client_secret=os.getenv('SPOTIFY_CLIENT_SECRET')
    )
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def fetch_trending_tracks(client: spotipy.Spotify, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch trending tracks from Spotify.
    
    Args:
        client: Spotify client instance
        limit: Maximum number of tracks to fetch
        
    Returns:
        List of track dictionaries
    """
    # Get featured playlists
    playlists_response = client.featured_playlists(limit=5)
    if not playlists_response or 'playlists' not in playlists_response:
        return []
    
    playlists = playlists_response['playlists']['items']
    
    tracks = []
    for playlist in playlists:
        # Get tracks from each playlist
        results = client.playlist_tracks(playlist['id'], limit=limit//5)
        if results and 'items' in results:
            tracks.extend([item['track'] for item in results['items'] if item.get('track')])
    
    return tracks

def upload_to_gcs(data: Dict[str, Any], bucket_name: str, blob_path: str) -> str:
    """
    Upload data to Google Cloud Storage.
    
    Args:
        data: Data to upload
        bucket_name: GCS bucket name
        blob_path: Path within the bucket
        
    Returns:
        GCS URI of the uploaded file
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type='application/json'
    )
    
    return f"gs://{bucket_name}/{blob_path}"

def fetch_spotify_data(**context) -> str:
    """
    Fetch data from Spotify API and upload to GCS.
    
    Returns:
        GCS path where the data is stored
    """
    # Initialize Spotify client
    client = get_spotify_client()
    
    # Fetch trending tracks
    tracks_data = fetch_trending_tracks(client)
    
    # Extract unique artist IDs
    artist_ids = list(set(track['artists'][0]['id'] for track in tracks_data))
    
    # Fetch artist data
    artist_data = [client.artist(artist_id) for artist_id in artist_ids]
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    tracks_filename = f'spotify/raw/tracks_{timestamp}.json'
    artists_filename = f'spotify/raw/artists_{timestamp}.json'
    
    # Upload to GCS
    bucket_name = 'prysm-raw-data'
    tracks_path = upload_to_gcs({'tracks': tracks_data}, bucket_name, tracks_filename)
    artists_path = upload_to_gcs({'artists': artist_data}, bucket_name, artists_filename)
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='tracks_path', value=tracks_path)
    context['task_instance'].xcom_push(key='artists_path', value=artists_path)
    
    return tracks_path  # Return the tracks path as the main output 