import json
import pandas as pd
from google.cloud import storage
from datetime import datetime

def read_from_gcs(bucket_name, blob_path):
    """Read JSON data from Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Download and parse JSON data
    content = blob.download_as_string()
    return json.loads(content)

def process_spotify_data(tracks_data, artists_data):
    """Process Spotify tracks and artists data."""
    # Convert to DataFrames
    tracks_df = pd.DataFrame(tracks_data)
    artists_df = pd.DataFrame(artists_data)
    
    # Extract relevant features from tracks
    processed_tracks = tracks_df.apply(lambda x: {
        'id': x.get('id'),
        'name': x.get('name'),
        'popularity': x.get('popularity'),
        'artist_id': x['artists'][0]['id'] if x.get('artists') else None,
        'artist_name': x['artists'][0]['name'] if x.get('artists') else None,
        'album': x.get('album', {}).get('name'),
        'release_date': x.get('album', {}).get('release_date'),
        'duration_ms': x.get('duration_ms'),
        'explicit': x.get('explicit', False),
        'timestamp': datetime.now().isoformat()
    }, axis=1).tolist()
    
    # Extract relevant features from artists
    processed_artists = artists_df.apply(lambda x: {
        'id': x.get('id'),
        'name': x.get('name'),
        'genres': x.get('genres', []),
        'popularity': x.get('popularity'),
        'followers': x.get('followers', {}).get('total'),
        'timestamp': datetime.now().isoformat()
    }, axis=1).tolist()
    
    return processed_tracks, processed_artists

def process_trends_data(topics_data, interest_data, related_data):
    """Process Google Trends data."""
    # Process trending topics
    processed_topics = []
    for topic in topics_data:
        processed_topic = {
            'topic': topic['topic'],
            'interest_over_time': topic.get('interest_over_time', {}),
            'timestamp': topic.get('timestamp', datetime.now().isoformat())
        }
        processed_topics.append(processed_topic)
    
    # Process interest data
    processed_interest = {
        'data': interest_data,
        'timestamp': datetime.now().isoformat()
    }
    
    # Process related topics
    processed_related = {
        'data': related_data,
        'timestamp': datetime.now().isoformat()
    }
    
    return processed_topics, processed_interest, processed_related

def process_linkedin_data(profile_data, connections_data, activity_data):
    """Process LinkedIn data."""
    # Process profile data
    processed_profile = {
        'id': profile_data.get('id'),
        'first_name': profile_data.get('firstName', {}).get('localized', {}).get('en_US'),
        'last_name': profile_data.get('lastName', {}).get('localized', {}).get('en_US'),
        'headline': profile_data.get('headline', {}).get('localized', {}).get('en_US'),
        'industry': profile_data.get('industry', {}).get('localized', {}).get('en_US'),
        'timestamp': datetime.now().isoformat()
    }
    
    # Process connections data
    processed_connections = []
    for connection in connections_data.get('elements', []):
        processed_connection = {
            'id': connection.get('id'),
            'first_name': connection.get('firstName', {}).get('localized', {}).get('en_US'),
            'last_name': connection.get('lastName', {}).get('localized', {}).get('en_US'),
            'headline': connection.get('headline', {}).get('localized', {}).get('en_US'),
            'industry': connection.get('industry', {}).get('localized', {}).get('en_US'),
            'timestamp': datetime.now().isoformat()
        }
        processed_connections.append(processed_connection)
    
    # Process activity data
    processed_activities = []
    for activity in activity_data.get('elements', []):
        processed_activity = {
            'id': activity.get('id'),
            'type': activity.get('type'),
            'timestamp': activity.get('created', {}).get('time'),
            'text': activity.get('text', {}).get('text'),
            'processed_timestamp': datetime.now().isoformat()
        }
        processed_activities.append(processed_activity)
    
    return processed_profile, processed_connections, processed_activities

def upload_processed_data(data, bucket_name, blob_path):
    """Upload processed data to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Convert data to JSON string
    json_data = json.dumps(data)
    
    # Upload to GCS
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 