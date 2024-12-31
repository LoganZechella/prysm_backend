import json
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from google.cloud import storage

def get_google_trends_client():
    """Initialize and return a Google Trends client."""
    return TrendReq(hl='en-US', tz=360)

def fetch_trending_topics(client, category=1113):  # 1113 is the category ID for music
    """Fetch trending topics from Google Trends in the music category."""
    # Get real-time trending searches
    trending = client.trending_searches(pn='united_states')
    
    # Get more details about each trending topic
    topics_data = []
    for topic in trending[:10]:  # Limit to top 10 trending topics
        client.build_payload([topic], timeframe='today 3-m', cat=category)
        interest_data = client.interest_over_time()
        
        if not interest_data.empty:
            topic_data = {
                'topic': topic,
                'interest_over_time': interest_data[topic].to_dict(),
                'timestamp': datetime.now().isoformat()
            }
            topics_data.append(topic_data)
    
    return topics_data

def fetch_topic_interest(client, topics, timeframe='today 3-m'):
    """Fetch interest over time data for specific topics."""
    interest_data = {}
    
    # Process topics in batches of 5 (API limitation)
    for i in range(0, len(topics), 5):
        batch = topics[i:i+5]
        client.build_payload(batch, timeframe=timeframe)
        data = client.interest_over_time()
        
        if not data.empty:
            for topic in batch:
                interest_data[topic] = data[topic].to_dict()
    
    return interest_data

def fetch_related_topics(client, topics):
    """Fetch related topics for the given topics."""
    related_data = {}
    
    for topic in topics:
        client.build_payload([topic])
        related = client.related_topics()
        
        if related and topic in related:
            # Convert DataFrame to dict and keep only relevant columns
            related_data[topic] = related[topic]['rising'].to_dict('records')
    
    return related_data

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