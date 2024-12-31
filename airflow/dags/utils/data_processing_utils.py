import json
from google.cloud import storage
from datetime import datetime
from typing import Dict, List, Any, Tuple

def read_from_gcs(bucket_name: str, blob_path: str) -> Any:
    """
    Read JSON data from Google Cloud Storage.
    
    Args:
        bucket_name: Name of the GCS bucket
        blob_path: Path to the blob within the bucket
        
    Returns:
        Parsed JSON data
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    content = blob.download_as_string()
    return json.loads(content)

def process_spotify_data(tracks_data: List[Dict], artists_data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Process raw Spotify data into a more analytics-friendly format.
    
    Args:
        tracks_data: Raw tracks data
        artists_data: Raw artists data
        
    Returns:
        Tuple of processed tracks and artists data
    """
    processed_tracks = []
    processed_artists = []
    
    # Process tracks
    for track in tracks_data:
        processed_track = {
            'id': track['id'],
            'name': track['name'],
            'popularity': track['popularity'],
            'duration_ms': track['duration_ms'],
            'explicit': track['explicit'],
            'artist_ids': [artist['id'] for artist in track['artists']],
            'artist_names': [artist['name'] for artist in track['artists']],
            'album_name': track['album']['name'],
            'album_release_date': track['album']['release_date'],
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_tracks.append(processed_track)
    
    # Process artists
    for artist in artists_data:
        processed_artist = {
            'id': artist['id'],
            'name': artist['name'],
            'popularity': artist['popularity'],
            'genres': artist['genres'],
            'followers': artist['followers']['total'],
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_artists.append(processed_artist)
    
    return processed_tracks, processed_artists

def process_trends_data(
    trending_data: List[Dict],
    interest_data: Dict[str, Dict],
    related_data: Dict[str, Dict]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Process raw Google Trends data into a more analytics-friendly format.
    
    Args:
        trending_data: Raw trending topics data
        interest_data: Raw interest over time data
        related_data: Raw related topics data
        
    Returns:
        Tuple of processed trending, interest, and related topics data
    """
    processed_trending = []
    processed_interest = []
    processed_related = []
    
    # Process trending topics
    for topic in trending_data:
        processed_topic = {
            'topic': topic['topic'],
            'rank': topic['rank'],
            'geo': topic['geo'],
            'timestamp': topic['timestamp']
        }
        processed_trending.append(processed_topic)
    
    # Process interest over time
    for topic, data in interest_data.items():
        for date, value in data['values'].items():
            interest_point = {
                'topic': topic,
                'date': date,
                'interest_value': value,
                'timeframe': data['timeframe'],
                'timestamp': data['timestamp']
            }
            processed_interest.append(interest_point)
    
    # Process related topics
    for topic, data in related_data.items():
        # Process rising topics
        for rising_topic in data['rising']:
            related_topic = {
                'main_topic': topic,
                'related_topic': rising_topic['topic_title'],
                'type': 'rising',
                'value': rising_topic['value'],
                'timestamp': data['timestamp']
            }
            processed_related.append(related_topic)
        
        # Process top topics
        for top_topic in data['top']:
            related_topic = {
                'main_topic': topic,
                'related_topic': top_topic['topic_title'],
                'type': 'top',
                'value': top_topic['value'],
                'timestamp': data['timestamp']
            }
            processed_related.append(related_topic)
    
    return processed_trending, processed_interest, processed_related

def process_linkedin_data(
    company_info: Dict,
    company_updates: List[Dict],
    job_postings: List[Dict]
) -> Tuple[Dict, List[Dict], List[Dict]]:
    """
    Process raw LinkedIn data into a more analytics-friendly format.
    
    Args:
        company_info: Raw company information
        company_updates: Raw company updates/posts
        job_postings: Raw job postings
        
    Returns:
        Tuple of processed company info, updates, and job postings
    """
    # Process company info
    processed_company = {
        'company_id': company_info['company_id'],
        'name': company_info['name'],
        'industry': company_info['industry'],
        'company_size': company_info['company_size'],
        'followers': company_info['followers'],
        'specialties': company_info['specialties'],
        'headquarters': next(
            (loc for loc in company_info['locations'] if loc['headquarters']),
            {'city': '', 'country': ''}
        ),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    # Process company updates
    processed_updates = []
    for update in company_updates:
        processed_update = {
            'update_id': update['update_id'],
            'company_id': update['company_id'],
            'content_length': len(update['content']),
            'engagement_total': sum(update['engagement'].values()),
            'likes': update['engagement']['likes'],
            'comments': update['engagement']['comments'],
            'shares': update['engagement']['shares'],
            'posted_at': update['posted_at'],
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_updates.append(processed_update)
    
    # Process job postings
    processed_jobs = []
    for job in job_postings:
        processed_job = {
            'job_id': job['job_id'],
            'company_id': job['company_id'],
            'title': job['title'],
            'location': job['location'],
            'listed_at': job['listed_at'],
            'engagement_total': job['applies'] + job['views'],
            'applies': job['applies'],
            'views': job['views'],
            'remote_allowed': job['remote_allowed'],
            'experience_level': job['experience_level'],
            'employment_type': job['employment_type'],
            'industries': job['industries'],
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_jobs.append(processed_job)
    
    return processed_company, processed_updates, processed_jobs

def upload_processed_data(data: Any, bucket_name: str, blob_path: str) -> str:
    """
    Upload processed data to Google Cloud Storage.
    
    Args:
        data: Data to upload
        bucket_name: Name of the GCS bucket
        blob_path: Path within the bucket
        
    Returns:
        GCS URI of the uploaded file
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    json_data = json.dumps(data, indent=2)
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 