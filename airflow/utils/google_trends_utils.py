import os
import json
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from google.cloud import storage

def get_google_trends_client():
    """
    Create and return a Google Trends client.
    """
    return TrendReq(hl='en-US', tz=360)

def fetch_trending_topics(pytrends_client, num_topics=20):
    """
    Fetch trending topics from Google Trends.
    """
    # Get trending searches for US
    trending_searches = pytrends_client.trending_searches(pn='united_states')
    
    # Convert to list of topics
    topics = trending_searches.values.tolist()[:num_topics]
    
    # Format the data
    trending_data = []
    for topic in topics:
        topic_data = {
            'topic': topic[0],
            'timestamp': datetime.utcnow().isoformat(),
            'region': 'US'
        }
        trending_data.append(topic_data)
    
    return trending_data

def fetch_topic_interest(pytrends_client, topics, timeframe='today 3-m'):
    """
    Fetch interest over time for specific topics.
    """
    interest_data = []
    
    # Process topics in batches of 5 (Google Trends limit)
    batch_size = 5
    for i in range(0, len(topics), batch_size):
        batch = topics[i:i + batch_size]
        
        # Build the payload
        pytrends_client.build_payload(
            kw_list=batch,
            timeframe=timeframe,
            geo='US'
        )
        
        # Get interest over time
        interest_over_time = pytrends_client.interest_over_time()
        
        if not interest_over_time.empty:
            # Convert to records
            for index, row in interest_over_time.iterrows():
                for topic in batch:
                    if topic in row:
                        data_point = {
                            'topic': topic,
                            'date': index.isoformat(),
                            'interest': int(row[topic]),
                            'timestamp': datetime.utcnow().isoformat()
                        }
                        interest_data.append(data_point)
    
    return interest_data

def fetch_related_topics(pytrends_client, topics):
    """
    Fetch related topics for each trending topic.
    """
    related_data = []
    
    for topic in topics:
        # Build the payload
        pytrends_client.build_payload(
            kw_list=[topic],
            timeframe='today 3-m',
            geo='US'
        )
        
        # Get related topics
        related = pytrends_client.related_topics()
        
        if related and topic in related:
            top_related = related[topic]['top']
            if not top_related.empty:
                for _, row in top_related.iterrows():
                    related_topic = {
                        'main_topic': topic,
                        'related_topic': row['topic_title'],
                        'related_type': row['topic_type'],
                        'value': int(row['value']),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    related_data.append(related_topic)
    
    return related_data

def upload_to_gcs(data, bucket_name, blob_path):
    """
    Upload data to Google Cloud Storage.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type='application/json'
    )
    
    return f"gs://{bucket_name}/{blob_path}" 