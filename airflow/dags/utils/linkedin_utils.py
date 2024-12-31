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

def fetch_profile_info(auth: Dict, profile_id: str = 'me') -> Dict:
    """
    Fetch basic profile information using LinkedIn's REST API.
    Uses the r_liteprofile scope.
    
    Args:
        auth: LinkedIn OAuth credentials
        profile_id: LinkedIn profile ID (default: 'me' for authenticated user)
        
    Returns:
        Dictionary containing profile information
    """
    # LinkedIn v2 API endpoint for lite profile
    url = 'https://api.linkedin.com/v2/me'
    
    # Fields available with r_liteprofile scope
    fields = [
        'id',
        'localizedFirstName',
        'localizedLastName',
        'profilePicture(displayImage~:playableStreams)'
    ]
    
    params = {
        'projection': f'({",".join(fields)})'
    }
    
    response = requests.get(url, headers=auth['headers'], params=params)
    response.raise_for_status()
    profile = response.json()
    
    # Get email address (requires r_emailaddress scope)
    email_url = 'https://api.linkedin.com/v2/emailAddress?q=members&projection=(elements*(handle~))'
    try:
        email_response = requests.get(email_url, headers=auth['headers'])
        email_response.raise_for_status()
        email_data = email_response.json()
        email = email_data.get('elements', [{}])[0].get('handle~', {}).get('emailAddress', '')
    except:
        email = ''
    
    # Process the response into our standard format
    processed_info = {
        'profile_id': profile.get('id', ''),
        'first_name': profile.get('localizedFirstName', ''),
        'last_name': profile.get('localizedLastName', ''),
        'email': email,
        'profile_picture_url': (
            profile.get('profilePicture', {})
            .get('displayImage~', {})
            .get('elements', [{}])[0]
            .get('identifiers', [{}])[0]
            .get('identifier', '')
        ),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    return processed_info

def fetch_connections(auth: Dict) -> List[Dict]:
    """
    Fetch user's first-degree connections using LinkedIn's REST API.
    Uses the r_network scope.
    
    Args:
        auth: LinkedIn OAuth credentials
        
    Returns:
        List of connections with basic information
    """
    url = 'https://api.linkedin.com/v2/connections'
    params = {
        'q': 'viewer',
        'start': 0,
        'count': 50,  # LinkedIn API limit
        'projection': '(elements*(miniProfile))'
    }
    
    try:
        response = requests.get(url, headers=auth['headers'], params=params)
        response.raise_for_status()
        data = response.json()
        
        processed_connections = []
        for connection in data.get('elements', []):
            mini_profile = connection.get('miniProfile', {})
            conn_data = {
                'profile_id': mini_profile.get('id', ''),
                'first_name': mini_profile.get('firstName', {}).get('localized', {}).get('en_US', ''),
                'last_name': mini_profile.get('lastName', {}).get('localized', {}).get('en_US', ''),
                'occupation': mini_profile.get('occupation', {}).get('localized', {}).get('en_US', ''),
                'public_identifier': mini_profile.get('publicIdentifier', ''),
                'timestamp': datetime.utcnow().isoformat()
            }
            processed_connections.append(conn_data)
        
        return processed_connections
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            # Return empty list if we don't have permission
            return []
        raise

def fetch_recent_activity(auth: Dict) -> List[Dict]:
    """
    Fetch user's recent activity using LinkedIn's REST API.
    Uses the r_basicprofile scope.
    
    Args:
        auth: LinkedIn OAuth credentials
        
    Returns:
        List of recent activities
    """
    url = 'https://api.linkedin.com/v2/shares'
    params = {
        'q': 'owners',
        'owners': f'urn:li:person:{auth.get("profile_id", "me")}',
        'count': 50
    }
    
    try:
        response = requests.get(url, headers=auth['headers'], params=params)
        response.raise_for_status()
        data = response.json()
        
        processed_activities = []
        for activity in data.get('elements', []):
            activity_data = {
                'activity_id': activity.get('id', ''),
                'activity_type': activity.get('activity', ''),
                'message': (
                    activity.get('specificContent', {})
                    .get('com.linkedin.ugc.ShareContent', {})
                    .get('message', {})
                    .get('text', '')
                ),
                'created_time': activity.get('created', {}).get('time', ''),
                'last_modified_time': activity.get('lastModified', {}).get('time', ''),
                'timestamp': datetime.utcnow().isoformat()
            }
            processed_activities.append(activity_data)
        
        return processed_activities
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            # Return empty list if we don't have permission
            return []
        raise

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
    
    json_data = json.dumps(data, indent=2)
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 