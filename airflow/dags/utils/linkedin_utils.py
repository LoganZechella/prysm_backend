import os
import json
from datetime import datetime
from typing import Dict, List, Any
import requests
from google.cloud import storage

def get_linkedin_client() -> Dict:
    """
    Initialize and return LinkedIn OAuth credentials.
    
    Returns:
        Dictionary containing access token and other OAuth info
    """
    client_id = os.getenv('LINKEDIN_CLIENT_ID')
    client_secret = os.getenv('LINKEDIN_CLIENT_SECRET')
    access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
    
    if not all([client_id, client_secret, access_token]):
        raise ValueError("LinkedIn OAuth credentials not found in environment variables")
    
    return {
        'access_token': access_token,
        'headers': {
            'Authorization': f'Bearer {access_token}',
            'X-Restli-Protocol-Version': '2.0.0',
            'Content-Type': 'application/json'
        }
    }

def fetch_profile_info(auth: Dict) -> Dict:
    """
    Fetch user profile information using LinkedIn's REST API.
    
    Args:
        auth: LinkedIn OAuth credentials
        
    Returns:
        Dictionary containing user profile information
    """
    # Fetch basic profile info
    profile_url = 'https://api.linkedin.com/v2/me'
    profile_fields = [
        'id',
        'localizedFirstName',
        'localizedLastName',
        'profilePicture'
    ]
    
    params = {
        'projection': f'({",".join(profile_fields)})'
    }
    
    response = requests.get(profile_url, headers=auth['headers'], params=params)
    response.raise_for_status()
    profile = response.json()
    
    # Fetch email address
    email_url = 'https://api.linkedin.com/v2/emailAddress?q=members&projection=(elements*(handle~))'
    email_response = requests.get(email_url, headers=auth['headers'])
    email_response.raise_for_status()
    email_data = email_response.json()
    
    # Extract profile picture URL if available
    profile_picture_url = None
    if 'profilePicture' in profile:
        picture_data = profile['profilePicture'].get('displayImage~', {}).get('elements', [])
        if picture_data:
            identifiers = picture_data[0].get('identifiers', [])
            if identifiers:
                profile_picture_url = identifiers[0].get('identifier')
    
    # Extract email from response
    email = None
    if 'elements' in email_data and email_data['elements']:
        email = email_data['elements'][0].get('handle~', {}).get('emailAddress')
    
    processed_info = {
        'profile_id': profile.get('id'),
        'first_name': profile.get('localizedFirstName'),
        'last_name': profile.get('localizedLastName'),
        'email': email,
        'profile_picture_url': profile_picture_url,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    return processed_info

def fetch_connections(auth: Dict, limit: int = 50) -> List[Dict]:
    """
    Fetch user's connections using LinkedIn's REST API.
    
    Args:
        auth: LinkedIn OAuth credentials
        limit: Maximum number of connections to fetch
        
    Returns:
        List of connections with basic profile information
    """
    url = 'https://api.linkedin.com/v2/connections'
    params = {
        'q': 'viewer',
        'count': limit,
        'projection': '(elements*(miniProfile))'
    }
    
    response = requests.get(url, headers=auth['headers'], params=params)
    response.raise_for_status()
    data = response.json()
    
    processed_connections = []
    for connection in data.get('elements', []):
        mini_profile = connection.get('miniProfile', {})
        connection_data = {
            'profile_id': mini_profile.get('id'),
            'first_name': mini_profile.get('firstName', {}).get('localized', {}).get('en_US'),
            'last_name': mini_profile.get('lastName', {}).get('localized', {}).get('en_US'),
            'occupation': mini_profile.get('occupation', {}).get('localized', {}).get('en_US'),
            'public_identifier': mini_profile.get('publicIdentifier'),
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_connections.append(connection_data)
    
    return processed_connections

def fetch_recent_activity(auth: Dict, limit: int = 50) -> List[Dict]:
    """
    Fetch user's recent activity using LinkedIn's REST API.
    
    Args:
        auth: LinkedIn OAuth credentials
        limit: Maximum number of activities to fetch
        
    Returns:
        List of recent activities
    """
    url = 'https://api.linkedin.com/v2/shares'
    params = {
        'q': 'owners',
        'owners': 'urn:li:person:{profile_id}',
        'count': limit
    }
    
    response = requests.get(url, headers=auth['headers'], params=params)
    response.raise_for_status()
    data = response.json()
    
    processed_activities = []
    for activity in data.get('elements', []):
        activity_data = {
            'activity_id': activity.get('id'),
            'activity_type': activity.get('activity'),
            'message': (
                activity.get('specificContent', {})
                .get('com.linkedin.ugc.ShareContent', {})
                .get('message', {})
                .get('text')
            ),
            'created_time': activity.get('created', {}).get('time'),
            'last_modified_time': activity.get('lastModified', {}).get('time'),
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_activities.append(activity_data)
    
    return processed_activities

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
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type='application/json'
    )
    
    return f'gs://{bucket_name}/{blob_path}' 