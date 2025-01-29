"""
Utility functions for processing data from various sources.
"""

import json
from typing import Dict, List, Tuple, Any
from datetime import datetime
from google.cloud import storage

def read_from_gcs(bucket_name: str, blob_path: str) -> Dict[str, Any]:
    """
    Read JSON data from Google Cloud Storage.
    
    Args:
        bucket_name: Name of the GCS bucket
        blob_path: Path to the blob in the bucket
        
    Returns:
        Parsed JSON data
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    data = blob.download_as_string()
    return json.loads(data)

def process_spotify_data(
    tracks_data: List[Dict[str, Any]],
    artists_data: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process Spotify tracks and artists data.
    
    Args:
        tracks_data: List of raw track data from Spotify API
        artists_data: List of raw artist data from Spotify API
        
    Returns:
        Tuple of (processed tracks, processed artists)
    """
    processed_tracks = []
    for track in tracks_data:
        processed_track = {
            'id': track['id'],
            'name': track['name'],
            'popularity': track['popularity'],
            'artist_id': track['artists'][0]['id'] if track['artists'] else None,
            'artist_name': track['artists'][0]['name'] if track['artists'] else None,
            'album_name': track['album']['name'],
            'release_date': track['album']['release_date'],
            'duration_ms': track['duration_ms'],
            'explicit': track['explicit']
        }
        processed_tracks.append(processed_track)
        
    processed_artists = []
    for artist in artists_data:
        processed_artist = {
            'id': artist['id'],
            'name': artist['name'],
            'genres': artist['genres'],
            'popularity': artist['popularity'],
            'followers': artist['followers']['total']
        }
        processed_artists.append(processed_artist)
        
    return processed_tracks, processed_artists

def process_trends_data(
    topics_data: List[Dict[str, Any]],
    interest_data: Dict[str, Dict[str, int]],
    related_data: Dict[str, List[str]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Process Google Trends data.
    
    Args:
        topics_data: List of topic data
        interest_data: Interest over time data
        related_data: Related topics data
        
    Returns:
        Tuple of (processed topics, processed interest data, processed related data)
    """
    processed_topics = []
    for topic in topics_data:
        processed_topic = {
            'topic': topic['topic'],
            'interest': topic['interest_over_time'],
            'timestamp': topic['timestamp']
        }
        processed_topics.append(processed_topic)
        
    processed_interest = {'data': interest_data}
    processed_related = {'data': related_data}
    
    return processed_topics, processed_interest, processed_related

def process_linkedin_data(
    profile_data: Dict[str, Any],
    connections_data: Dict[str, Any],
    activity_data: Dict[str, Any]
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Process LinkedIn data.
    
    Args:
        profile_data: Raw profile data
        connections_data: Raw connections data
        activity_data: Raw activity data
        
    Returns:
        Tuple of (processed profile, processed connections, processed activities)
    """
    processed_profile = {
        'id': profile_data['id'],
        'first_name': profile_data['firstName']['localized']['en_US'],
        'last_name': profile_data['lastName']['localized']['en_US'],
        'headline': profile_data['headline']['localized']['en_US'],
        'industry': profile_data['industry']['localized']['en_US']
    }
    
    processed_connections = []
    for connection in connections_data['elements']:
        processed_connection = {
            'id': connection['id'],
            'first_name': connection['firstName']['localized']['en_US'],
            'last_name': connection['lastName']['localized']['en_US'],
            'headline': connection['headline']['localized']['en_US'],
            'industry': connection['industry']['localized']['en_US']
        }
        processed_connections.append(processed_connection)
        
    processed_activities = []
    for activity in activity_data['elements']:
        processed_activity = {
            'id': activity['id'],
            'type': activity['type'],
            'created_at': activity['created']['time'],
            'text': activity['text']['text']
        }
        processed_activities.append(processed_activity)
        
    return processed_profile, processed_connections, processed_activities

def upload_processed_data(data: Dict[str, Any], bucket_name: str, blob_path: str) -> str:
    """
    Upload processed data to Google Cloud Storage.
    
    Args:
        data: Data to upload
        bucket_name: Name of the GCS bucket
        blob_path: Path to the blob in the bucket
        
    Returns:
        GCS URI of the uploaded data
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(
        json.dumps(data),
        content_type='application/json'
    )
    
    return f'gs://{bucket_name}/{blob_path}' 