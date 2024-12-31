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

def fetch_company_info(auth: Dict, company_id: str) -> Dict:
    """
    Fetch company information using LinkedIn's REST API.
    
    Args:
        auth: LinkedIn OAuth credentials
        company_id: LinkedIn company ID
        
    Returns:
        Dictionary containing company information
    """
    url = f'https://api.linkedin.com/v2/organizations/{company_id}'
    
    fields = [
        'id',
        'name',
        'description',
        'industries',
        'specialties',
        'locations',
        'staffCount',
        'foundedOn',
        'website',
        'groups',
        'status',
        'vanityName'
    ]
    
    params = {
        'projection': f'({",".join(fields)})'
    }
    
    response = requests.get(url, headers=auth['headers'], params=params)
    response.raise_for_status()
    company = response.json()
    
    processed_info = {
        'company_id': company.get('id', ''),
        'name': company.get('name', ''),
        'description': company.get('description', ''),
        'industries': company.get('industries', []),
        'specialties': company.get('specialties', []),
        'locations': company.get('locations', []),
        'staff_count': company.get('staffCount', 0),
        'founded_on': company.get('foundedOn', ''),
        'website': company.get('website', ''),
        'status': company.get('status', ''),
        'vanity_name': company.get('vanityName', ''),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    return processed_info

def fetch_company_updates(auth: Dict, company_id: str, limit: int = 50) -> List[Dict]:
    """
    Fetch company updates/posts using LinkedIn's REST API.
    
    Args:
        auth: LinkedIn OAuth credentials
        company_id: LinkedIn company ID
        limit: Maximum number of updates to fetch
        
    Returns:
        List of company updates
    """
    url = f'https://api.linkedin.com/v2/shares'
    
    params = {
        'q': 'owners',
        'owners': f'urn:li:organization:{company_id}',
        'count': limit
    }
    
    response = requests.get(url, headers=auth['headers'], params=params)
    response.raise_for_status()
    data = response.json()
    
    processed_updates = []
    for update in data.get('elements', []):
        update_data = {
            'update_id': update.get('id', ''),
            'company_id': company_id,
            'content': (
                update.get('specificContent', {})
                .get('com.linkedin.ugc.ShareContent', {})
                .get('shareCommentary', {})
                .get('text', '')
            ),
            'engagement': {
                'likes': update.get('social', {}).get('totalLikes', 0),
                'comments': update.get('social', {}).get('totalComments', 0),
                'shares': update.get('social', {}).get('totalShares', 0)
            },
            'posted_at': update.get('created', {}).get('time', ''),
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_updates.append(update_data)
    
    return processed_updates

def fetch_job_postings(auth: Dict, company_id: str, limit: int = 50) -> List[Dict]:
    """
    Fetch job postings using LinkedIn's REST API.
    
    Args:
        auth: LinkedIn OAuth credentials
        company_id: LinkedIn company ID
        limit: Maximum number of jobs to fetch
        
    Returns:
        List of job postings
    """
    url = f'https://api.linkedin.com/v2/jobs'
    
    params = {
        'q': 'companyId',
        'companyId': company_id,
        'count': limit
    }
    
    response = requests.get(url, headers=auth['headers'], params=params)
    response.raise_for_status()
    data = response.json()
    
    processed_jobs = []
    for job in data.get('elements', []):
        job_data = {
            'job_id': job.get('id', ''),
            'company_id': company_id,
            'title': job.get('title', ''),
            'description': job.get('description', ''),
            'location': job.get('locationDescription', ''),
            'remote_allowed': job.get('workRemoteAllowed', False),
            'experience_level': job.get('experienceLevel', ''),
            'employment_type': job.get('employmentType', ''),
            'industries': job.get('industries', []),
            'applies': job.get('applies', 0),
            'views': job.get('views', 0),
            'listed_at': job.get('listedAt', ''),
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_jobs.append(job_data)
    
    return processed_jobs

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