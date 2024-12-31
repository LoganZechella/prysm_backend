import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
from google.cloud import storage

def get_spotify_client():
    """
    Create and return an authenticated Spotify client.
    """
    client_id = os.getenv('SPOTIFY_CLIENT_ID')
    client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        raise ValueError("Spotify credentials not found in environment variables")
    
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def fetch_trending_tracks(sp_client, limit=50):
    """
    Fetch trending tracks from Spotify charts.
    """
    # Get some popular artists first
    seed_artists = [
        '06HL4z0CvFAxyc27GXpf02',  # Taylor Swift
        '3TVXtAsR1Inumwj472S9r4',  # Drake
        '1uNFoZAHBGtllmzznpCI3s',  # Justin Bieber
        '6eUKZXaKkcviH0Ku9w2n3V',  # Ed Sheeran
        '66CXWjxzNUsdJxJ2JdwvnR'   # Ariana Grande
    ]
    
    tracks = []
    for artist_id in seed_artists:
        # Get top tracks for each artist
        results = sp_client.artist_top_tracks(artist_id, country='US')
        
        for track in results['tracks']:
            track_data = {
                'track_id': track['id'],
                'name': track['name'],
                'artists': [{'name': artist['name'], 'id': artist['id']} for artist in track['artists']],
                'album_name': track['album']['name'],
                'album_id': track['album']['id'],
                'popularity': track['popularity'],
                'timestamp': datetime.utcnow().isoformat()
            }
            tracks.append(track_data)
    
    return tracks

def fetch_artist_data(sp_client, artist_ids):
    """
    Fetch detailed artist information.
    """
    artists_data = []
    
    chunk_size = 50
    for i in range(0, len(artist_ids), chunk_size):
        chunk = artist_ids[i:i + chunk_size]
        results = sp_client.artists(chunk)
        
        for artist in results['artists']:
            artist_data = {
                'artist_id': artist['id'],
                'name': artist['name'],
                'genres': artist['genres'],
                'popularity': artist['popularity'],
                'followers': artist['followers']['total'],
                'timestamp': datetime.utcnow().isoformat()
            }
            artists_data.append(artist_data)
    
    return artists_data

def test_spotify_ingestion():
    print("Starting Spotify ingestion test...")
    
    # Test Spotify client creation
    try:
        sp_client = get_spotify_client()
        print("✓ Successfully created Spotify client")
    except Exception as e:
        print(f"✗ Failed to create Spotify client: {str(e)}")
        return
    
    # Test fetching trending tracks
    try:
        tracks = fetch_trending_tracks(sp_client)
        print(f"✓ Successfully fetched {len(tracks)} trending tracks")
    except Exception as e:
        print(f"✗ Failed to fetch trending tracks: {str(e)}")
        return
    
    # Extract and fetch artist data
    try:
        artist_ids = set()
        for track in tracks:
            for artist in track['artists']:
                if isinstance(artist, str):  # If it's just the name
                    continue
                if artist.get('id'):
                    artist_ids.add(artist['id'])
        
        artists = fetch_artist_data(sp_client, list(artist_ids))
        print(f"✓ Successfully fetched {len(artists)} artists")
    except Exception as e:
        print(f"✗ Failed to fetch artist data: {str(e)}")
        return
    
    # Test GCS upload
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(os.getenv('GCS_RAW_BUCKET'))
        
        # Upload tracks
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        tracks_blob = bucket.blob(f'spotify/raw/tracks/tracks_{timestamp}.json')
        tracks_blob.upload_from_string(
            json.dumps(tracks, indent=2),
            content_type='application/json'
        )
        print(f"✓ Successfully uploaded tracks to {tracks_blob.name}")
        
        # Upload artists
        artists_blob = bucket.blob(f'spotify/raw/artists/artists_{timestamp}.json')
        artists_blob.upload_from_string(
            json.dumps(artists, indent=2),
            content_type='application/json'
        )
        print(f"✓ Successfully uploaded artists to {artists_blob.name}")
        
    except Exception as e:
        print(f"✗ Failed to upload to GCS: {str(e)}")
        return
    
    print("\nTest completed successfully! 🎉")
    print(f"Tracks file: gs://{os.getenv('GCS_RAW_BUCKET')}/spotify/raw/tracks/tracks_{timestamp}.json")
    print(f"Artists file: gs://{os.getenv('GCS_RAW_BUCKET')}/spotify/raw/artists/artists_{timestamp}.json")

if __name__ == "__main__":
    test_spotify_ingestion() 