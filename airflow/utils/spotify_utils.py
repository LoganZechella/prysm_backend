import os
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def get_spotify_client():
    """
    Create and return an authenticated Spotify client.
    
    Returns:
        spotipy.Spotify: Authenticated Spotify client
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
    Fetch trending tracks from Spotify's featured playlists.
    
    Args:
        sp_client (spotipy.Spotify): Authenticated Spotify client
        limit (int): Maximum number of tracks to fetch per playlist
        
    Returns:
        list: List of track dictionaries with relevant information
    """
    # Get featured playlists
    playlists = sp_client.featured_playlists()['playlists']['items']
    
    tracks = []
    for playlist in playlists:
        # Get tracks from each playlist
        results = sp_client.playlist_tracks(
            playlist['id'],
            fields='items(track(id,name,artists,album(id,name),popularity))',
            limit=limit
        )
        
        for item in results['items']:
            if not item['track']:
                continue
                
            track = item['track']
            track_data = {
                'track_id': track['id'],
                'name': track['name'],
                'artists': [artist['name'] for artist in track['artists']],
                'album_name': track['album']['name'],
                'album_id': track['album']['id'],
                'popularity': track['popularity'],
                'playlist_name': playlist['name'],
                'playlist_id': playlist['id'],
                'timestamp': datetime.utcnow().isoformat()
            }
            tracks.append(track_data)
    
    return tracks

def fetch_artist_data(sp_client, artist_ids):
    """
    Fetch detailed artist information.
    
    Args:
        sp_client (spotipy.Spotify): Authenticated Spotify client
        artist_ids (list): List of Spotify artist IDs
        
    Returns:
        list: List of artist dictionaries with relevant information
    """
    artists_data = []
    
    # Spotify API allows fetching up to 50 artists at once
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

def save_to_json(data, filename):
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save
        filename (str): Name of the file to save to
    """
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2) 