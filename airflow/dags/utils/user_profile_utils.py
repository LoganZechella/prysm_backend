import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

def create_user_profile(
    user_id: str,
    spotify_id: Optional[str] = None,
    linkedin_company_ids: Optional[List[str]] = None,
    trends_topics: Optional[List[str]] = None
) -> Dict:
    """
    Create or update a user profile with source-specific identifiers.
    
    Args:
        user_id: Unique identifier for the user
        spotify_id: User's Spotify ID
        linkedin_company_ids: List of LinkedIn company IDs to track
        trends_topics: List of Google Trends topics to track
        
    Returns:
        Created/updated user profile
    """
    profile = {
        'user_id': user_id,
        'spotify': {
            'id': spotify_id,
            'last_updated': datetime.utcnow().isoformat()
        },
        'linkedin': {
            'company_ids': linkedin_company_ids or [],
            'last_updated': datetime.utcnow().isoformat()
        },
        'trends': {
            'topics': trends_topics or [],
            'last_updated': datetime.utcnow().isoformat()
        },
        'created_at': datetime.utcnow().isoformat(),
        'updated_at': datetime.utcnow().isoformat()
    }
    
    return profile

def store_user_profile(profile: Dict, bucket_name: str) -> str:
    """
    Store user profile in Google Cloud Storage.
    
    Args:
        profile: User profile data
        bucket_name: Name of the GCS bucket
        
    Returns:
        GCS URI of the stored profile
    """
    if not bucket_name:
        raise ValueError("bucket_name cannot be None or empty")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    user_id = profile['user_id']
    blob_path = f'user_profiles/{user_id}.json'
    blob = bucket.blob(blob_path)
    
    json_data = json.dumps(profile, indent=2)
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}'

def get_user_profile(user_id: str, bucket_name: str) -> Dict:
    """
    Retrieve user profile from Google Cloud Storage.
    
    Args:
        user_id: User ID to retrieve
        bucket_name: Name of the GCS bucket
        
    Returns:
        User profile data
    """
    if not bucket_name:
        raise ValueError("bucket_name cannot be None or empty")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob_path = f'user_profiles/{user_id}.json'
    blob = bucket.blob(blob_path)
    
    content = blob.download_as_string()
    return json.loads(content)

def aggregate_user_data(
    user_id: str,
    processed_bucket: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict:
    """
    Aggregate processed data from all sources for a specific user.
    
    Args:
        user_id: User ID to aggregate data for
        processed_bucket: Name of the GCS bucket containing processed data
        start_date: Optional start date for data range (ISO format)
        end_date: Optional end date for data range (ISO format)
        
    Returns:
        Aggregated user data from all sources
    """
    if not processed_bucket:
        raise ValueError("processed_bucket cannot be None or empty")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(processed_bucket)
    
    # Get user profile
    profile = get_user_profile(user_id, processed_bucket)
    
    aggregated_data = {
        'user_id': user_id,
        'spotify': {
            'tracks': [],
            'artists': []
        },
        'linkedin': {
            'companies': [],
            'updates': [],
            'jobs': []
        },
        'trends': {
            'trending': [],
            'interest': [],
            'related': []
        },
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Helper function to filter data by date range
    def is_in_date_range(timestamp: str) -> bool:
        if not (start_date or end_date):
            return True
        dt = datetime.fromisoformat(timestamp)
        if start_date and dt < datetime.fromisoformat(start_date):
            return False
        if end_date and dt > datetime.fromisoformat(end_date):
            return False
        return True
    
    # Aggregate Spotify data if user has Spotify ID
    if profile['spotify']['id']:
        spotify_id = profile['spotify']['id']
        
        # Get latest processed tracks and artists
        for blob in bucket.list_blobs(prefix='spotify/processed'):
            data = json.loads(blob.download_as_string())
            
            if 'tracks' in blob.name:
                # Filter tracks by user's artists
                aggregated_data['spotify']['tracks'].extend([
                    track for track in data
                    if is_in_date_range(track['timestamp'])
                ])
            
            elif 'artists' in blob.name:
                # Add all processed artists
                aggregated_data['spotify']['artists'].extend([
                    artist for artist in data
                    if is_in_date_range(artist['timestamp'])
                ])
    
    # Aggregate LinkedIn data if user has company IDs
    if profile['linkedin']['company_ids']:
        company_ids = profile['linkedin']['company_ids']
        
        for blob in bucket.list_blobs(prefix='linkedin/processed'):
            data = json.loads(blob.download_as_string())
            
            if 'companies' in blob.name:
                # Filter companies by user's company IDs
                aggregated_data['linkedin']['companies'].extend([
                    company for company in data
                    if company['company_id'] in company_ids
                    and is_in_date_range(company['timestamp'])
                ])
            
            elif 'updates' in blob.name:
                # Filter updates by user's company IDs
                aggregated_data['linkedin']['updates'].extend([
                    update for update in data
                    if update['company_id'] in company_ids
                    and is_in_date_range(update['timestamp'])
                ])
            
            elif 'jobs' in blob.name:
                # Filter jobs by user's company IDs
                aggregated_data['linkedin']['jobs'].extend([
                    job for job in data
                    if job['company_id'] in company_ids
                    and is_in_date_range(job['timestamp'])
                ])
    
    # Aggregate Google Trends data if user has topics
    if profile['trends']['topics']:
        topics = profile['trends']['topics']
        
        for blob in bucket.list_blobs(prefix='trends/processed'):
            data = json.loads(blob.download_as_string())
            
            if 'trending' in blob.name:
                # Filter trending topics
                aggregated_data['trends']['trending'].extend([
                    topic for topic in data
                    if topic['topic'] in topics
                    and is_in_date_range(topic['timestamp'])
                ])
            
            elif 'interest' in blob.name:
                # Filter interest data
                aggregated_data['trends']['interest'].extend([
                    point for point in data
                    if point['topic'] in topics
                    and is_in_date_range(point['timestamp'])
                ])
            
            elif 'related' in blob.name:
                # Filter related topics
                aggregated_data['trends']['related'].extend([
                    related for related in data
                    if related['main_topic'] in topics
                    and is_in_date_range(related['timestamp'])
                ])
    
    return aggregated_data

def store_aggregated_data(data: Dict, bucket_name: str) -> str:
    """
    Store aggregated user data in Google Cloud Storage.
    
    Args:
        data: Aggregated user data
        bucket_name: Name of the GCS bucket
        
    Returns:
        GCS URI of the stored data
    """
    if not bucket_name:
        raise ValueError("bucket_name cannot be None or empty")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    user_id = data['user_id']
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    blob_path = f'aggregated/{user_id}/data_{timestamp}.json'
    blob = bucket.blob(blob_path)
    
    json_data = json.dumps(data, indent=2)
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 