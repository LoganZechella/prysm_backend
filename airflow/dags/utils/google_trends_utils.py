from pytrends.request import TrendReq
import json
from google.cloud import storage
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any

def get_google_trends_client() -> TrendReq:
    """
    Initialize and return a Google Trends client.
    """
    return TrendReq(hl='en-US', tz=360)

def fetch_trending_topics(pytrends: TrendReq, geo: str = 'US') -> List[Dict[str, Any]]:
    """
    Fetch daily trending topics from Google Trends.
    
    Args:
        pytrends: Initialized pytrends client
        geo: Geographic location (default: 'US')
        
    Returns:
        List of trending topics with metadata
    """
    # Get trending searches for the day
    trending_searches = pytrends.trending_searches(pn=geo)
    
    # Convert to list of dictionaries with metadata
    trending_topics = []
    for topic in trending_searches[0].tolist():
        trending_topics.append({
            'topic': topic,
            'geo': geo,
            'timestamp': datetime.utcnow().isoformat(),
            'rank': len(trending_topics) + 1
        })
    
    return trending_topics

def fetch_topic_interest(pytrends: TrendReq, topics: List[str], timeframe: str = 'today 3-m') -> Dict[str, Any]:
    """
    Fetch interest over time data for given topics.
    
    Args:
        pytrends: Initialized pytrends client
        topics: List of topics to analyze
        timeframe: Time range for analysis (default: 'today 3-m')
        
    Returns:
        Dictionary containing interest over time data
    """
    interest_data = {}
    
    # Process topics in batches of 5 (API limitation)
    for i in range(0, len(topics), 5):
        batch = topics[i:i+5]
        pytrends.build_payload(batch, timeframe=timeframe)
        
        # Get interest over time
        interest_over_time = pytrends.interest_over_time()
        
        # Convert to dictionary format
        if not interest_over_time.empty:
            for topic in batch:
                if topic in interest_over_time.columns:
                    interest_data[topic] = {
                        'values': interest_over_time[topic].to_dict(),
                        'timeframe': timeframe,
                        'timestamp': datetime.utcnow().isoformat()
                    }
    
    return interest_data

def fetch_related_topics(pytrends: TrendReq, topics: List[str]) -> Dict[str, Any]:
    """
    Fetch related topics for given topics.
    
    Args:
        pytrends: Initialized pytrends client
        topics: List of topics to analyze
        
    Returns:
        Dictionary containing related topics data
    """
    related_data = {}
    
    for topic in topics:
        pytrends.build_payload([topic])
        
        # Get related topics
        related_topics = pytrends.related_topics()
        
        if topic in related_topics and related_topics[topic]:
            rising = related_topics[topic].get('rising', None)
            top = related_topics[topic].get('top', None)
            
            related_data[topic] = {
                'rising': rising.to_dict('records') if rising is not None else [],
                'top': top.to_dict('records') if top is not None else [],
                'timestamp': datetime.utcnow().isoformat()
            }
    
    return related_data

def upload_to_gcs(data: Any, bucket_name: str, blob_path: str) -> str:
    """
    Upload data to Google Cloud Storage.
    
    Args:
        data: Data to upload
        bucket_name: Name of the GCS bucket
        blob_path: Path within the bucket
        
    Returns:
        GCS URI of the uploaded file
    """
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Convert data to JSON string
    json_data = json.dumps(data)
    
    # Upload to GCS
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 