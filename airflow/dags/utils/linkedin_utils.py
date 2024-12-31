"""
LinkedIn API integration module.
NOTE: This module is pending Marketing Developer Platform approval.
Once approved, this will be updated to use the Marketing API endpoints
with Client Credentials flow (2-legged OAuth).
"""

import os
import json
from datetime import datetime
import requests
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_linkedin_client():
    """Initialize and return a LinkedIn client."""
    return {
        'access_token': os.getenv('LINKEDIN_ACCESS_TOKEN'),
        'headers': {
            'Authorization': f'Bearer {os.getenv("LINKEDIN_ACCESS_TOKEN")}',
            'Content-Type': 'application/json',
            'X-Restli-Protocol-Version': '2.0.0'
        }
    }

def fetch_profile_info(client):
    """Fetch user profile information."""
    response = requests.get(
        'https://api.linkedin.com/v2/me',
        headers=client['headers']
    )
    response.raise_for_status()
    return response.json()

def fetch_connections(client):
    """Fetch user's connections."""
    response = requests.get(
        'https://api.linkedin.com/v2/connections',
        headers=client['headers']
    )
    response.raise_for_status()
    return response.json()

def fetch_recent_activity(client):
    """Fetch user's recent activity."""
    response = requests.get(
        'https://api.linkedin.com/v2/shares?q=owners&owners=urn:li:person:{profile_id}',
        headers=client['headers']
    )
    response.raise_for_status()
    return response.json()

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