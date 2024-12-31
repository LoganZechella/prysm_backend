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

class LinkedInClient:
    """Simple LinkedIn client using Sign In with LinkedIn."""
    
    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    
    def get_profile(self):
        """Get basic profile information using Sign In with LinkedIn."""
        url = "https://api.linkedin.com/v2/me"
        fields = "id,firstName,lastName,profilePicture"
        response = requests.get(
            f"{url}?projection=({fields})",
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()
    
    def get_email(self):
        """Get email address using Sign In with LinkedIn."""
        url = "https://api.linkedin.com/v2/emailAddress"
        params = {
            "q": "members",
            "projection": "(elements*(handle~))"
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()

def get_linkedin_client():
    """Initialize LinkedIn client."""
    access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
    
    if not access_token:
        raise ValueError("LinkedIn access token not found in environment variables")
    
    return LinkedInClient(access_token)

def fetch_profile_info(client):
    """Fetch user profile information."""
    try:
        profile = client.get_profile()
        email_data = client.get_email()
        
        # Extract email from response
        email = None
        if 'elements' in email_data and email_data['elements']:
            email = email_data['elements'][0].get('handle~', {}).get('emailAddress')
        
        # Extract profile picture URL if available
        picture_url = None
        if 'profilePicture' in profile:
            picture_data = profile['profilePicture'].get('displayImage~', {}).get('elements', [])
            if picture_data:
                identifiers = picture_data[0].get('identifiers', [])
                if identifiers:
                    picture_url = identifiers[0].get('identifier')
        
        return {
            'id': profile.get('id'),
            'first_name': profile.get('firstName', {}).get('localized', {}).get('en_US'),
            'last_name': profile.get('lastName', {}).get('localized', {}).get('en_US'),
            'email': email,
            'picture_url': picture_url,
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error fetching profile info: {str(e)}")
        return {
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }

def fetch_recent_activity(client):
    """Fetch user's recent social activity."""
    # Note: Social activity requires additional permissions
    # For now, return empty activity list
    return {
        'message': 'Social activity fetching requires additional permissions',
        'activities': [],
        'timestamp': datetime.utcnow().isoformat()
    }

def upload_to_gcs(data, bucket_name, blob_path):
    """Upload data to Google Cloud Storage."""
    from google.cloud import storage
    
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Convert data to JSON string
    json_data = json.dumps(data)
    blob.upload_from_string(json_data, content_type='application/json')
    
    return blob_path 