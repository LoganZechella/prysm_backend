import os
from utils.google_trends_utils import (
    get_google_trends_client,
    fetch_trending_topics,
    fetch_topic_interest,
    fetch_related_queries,
    upload_to_gcs
)

def test_google_trends_ingestion():
    print("Starting Google Trends ingestion test...")
    
    # Test Google Trends client creation
    try:
        pytrends = get_google_trends_client()
        print("✓ Successfully created Google Trends client")
    except Exception as e:
        print(f"✗ Failed to create Google Trends client: {str(e)}")
        return
    
    # Test fetching trending topics
    try:
        trending_topics = fetch_trending_topics(pytrends)
        print(f"✓ Successfully fetched {len(trending_topics)} trending topics")
    except Exception as e:
        print(f"✗ Failed to fetch trending topics: {str(e)}")
        return
    
    # Extract topic queries for interest and related queries
    topics = [topic['query'] for topic in trending_topics[:5]]  # Use top 5 topics
    
    # Test fetching interest over time
    try:
        interest_data = fetch_topic_interest(pytrends, topics)
        print(f"✓ Successfully fetched interest data for {len(topics)} topics")
    except Exception as e:
        print(f"✗ Failed to fetch interest data: {str(e)}")
        return
    
    # Test fetching related queries
    try:
        related_data = fetch_related_queries(pytrends, topics)
        print(f"✓ Successfully fetched related queries for {len(topics)} topics")
    except Exception as e:
        print(f"✗ Failed to fetch related queries: {str(e)}")
        return
    
    # Test GCS upload
    try:
        bucket_name = os.getenv('GCS_RAW_BUCKET')
        
        # Upload trending topics
        topics_blob = upload_to_gcs(
            trending_topics,
            bucket_name,
            'google_trends/raw/trending_topics.json'
        )
        print(f"✓ Successfully uploaded trending topics to {topics_blob}")
        
        # Upload interest data
        interest_blob = upload_to_gcs(
            interest_data,
            bucket_name,
            'google_trends/raw/topic_interest.json'
        )
        print(f"✓ Successfully uploaded interest data to {interest_blob}")
        
        # Upload related queries
        related_blob = upload_to_gcs(
            related_data,
            bucket_name,
            'google_trends/raw/related_queries.json'
        )
        print(f"✓ Successfully uploaded related queries to {related_blob}")
        
    except Exception as e:
        print(f"✗ Failed to upload to GCS: {str(e)}")
        return
    
    print("\nTest completed successfully! 🎉")
    print(f"Topics file: {topics_blob}")
    print(f"Interest file: {interest_blob}")
    print(f"Related queries file: {related_blob}")

if __name__ == "__main__":
    test_google_trends_ingestion() 