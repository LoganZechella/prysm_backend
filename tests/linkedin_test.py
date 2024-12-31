import os
import pytest
from utils.linkedin_utils import get_linkedin_client, fetch_profile_info, fetch_recent_activity

def test_linkedin_ingestion():
    """Test LinkedIn data ingestion functionality."""
    print("Starting LinkedIn ingestion test...")
    
    try:
        # Test client creation
        client = get_linkedin_client()
        print("✓ Successfully created LinkedIn client")
        
        # Test profile info fetching
        try:
            profile_data = fetch_profile_info(client)
            print("✓ Successfully fetched profile info")
            print(f"Profile: {profile_data.get('first_name', '')} {profile_data.get('last_name', '')}")
        except Exception as e:
            print("✗ Failed to fetch profile info:", str(e))
        
        # Test recent activity fetching
        try:
            activity_data = fetch_recent_activity(client)
            print("✓ Successfully fetched recent activity")
            activities = activity_data.get('activities', [])
            print(f"Found {len(activities)} activities")
        except Exception as e:
            print("✗ Failed to fetch recent activity:", str(e))
            
    except Exception as e:
        print("✗ Test failed:", str(e))
        raise

if __name__ == '__main__':
    test_linkedin_ingestion() 