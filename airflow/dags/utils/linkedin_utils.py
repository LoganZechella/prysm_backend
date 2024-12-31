import os
import json
from datetime import datetime
from typing import Dict, List, Any
from linkedin_api import Linkedin
from google.cloud import storage

def get_linkedin_client() -> Linkedin:
    """
    Initialize and return a LinkedIn client.
    
    Returns:
        Authenticated LinkedIn client
    """
    username = os.getenv('LINKEDIN_USERNAME')
    password = os.getenv('LINKEDIN_PASSWORD')
    return Linkedin(username, password)

def fetch_company_updates(client: Linkedin, company_id: str) -> List[Dict]:
    """
    Fetch recent company updates/posts.
    
    Args:
        client: LinkedIn client
        company_id: LinkedIn company ID
        
    Returns:
        List of company updates with engagement metrics
    """
    updates = client.get_company_updates(company_id)
    
    processed_updates = []
    for update in updates:
        if 'value' in update and 'com.linkedin.voyager.feed.render.UpdateV2' in update['value']:
            update_data = update['value']['com.linkedin.voyager.feed.render.UpdateV2']
            
            # Extract engagement metrics if available
            social_detail = update_data.get('socialDetail', {})
            engagement = {
                'likes': social_detail.get('totalSocialActivityCounts', {}).get('numLikes', 0),
                'comments': social_detail.get('totalSocialActivityCounts', {}).get('numComments', 0),
                'shares': social_detail.get('totalSocialActivityCounts', {}).get('numShares', 0)
            }
            
            # Extract content
            content = ''
            if 'commentary' in update_data:
                content = update_data['commentary'].get('text', {}).get('text', '')
            
            processed_update = {
                'update_id': update.get('entityUrn', ''),
                'company_id': company_id,
                'content': content,
                'engagement': engagement,
                'timestamp': datetime.utcnow().isoformat(),
                'posted_at': update_data.get('timeStamp', '')
            }
            processed_updates.append(processed_update)
    
    return processed_updates

def fetch_company_info(client: Linkedin, company_id: str) -> Dict:
    """
    Fetch detailed company information.
    
    Args:
        client: LinkedIn client
        company_id: LinkedIn company ID
        
    Returns:
        Dictionary containing company information
    """
    company_info = client.get_company(company_id)
    
    processed_info = {
        'company_id': company_id,
        'name': company_info.get('name', ''),
        'description': company_info.get('description', ''),
        'website': company_info.get('website', ''),
        'industry': company_info.get('industry', ''),
        'company_size': company_info.get('staffCount', 0),
        'followers': company_info.get('followingInfo', {}).get('followerCount', 0),
        'specialties': company_info.get('specialties', []),
        'locations': [
            {
                'city': loc.get('city', ''),
                'country': loc.get('country', ''),
                'headquarters': loc.get('headquarters', False)
            }
            for loc in company_info.get('locations', [])
        ],
        'timestamp': datetime.utcnow().isoformat()
    }
    
    return processed_info

def fetch_job_postings(client: Linkedin, company_id: str) -> List[Dict]:
    """
    Fetch recent job postings from the company.
    
    Args:
        client: LinkedIn client
        company_id: LinkedIn company ID
        
    Returns:
        List of job postings
    """
    jobs = client.get_company_jobs(company_id)
    
    processed_jobs = []
    for job in jobs:
        job_data = {
            'job_id': job.get('entityUrn', ''),
            'company_id': company_id,
            'title': job.get('title', ''),
            'location': job.get('formattedLocation', ''),
            'listed_at': job.get('listedAt', ''),
            'applies': job.get('applies', 0),
            'views': job.get('views', 0),
            'remote_allowed': job.get('workRemoteAllowed', False),
            'experience_level': job.get('experienceLevel', ''),
            'employment_type': job.get('employmentType', ''),
            'industries': job.get('industries', []),
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
    
    json_data = json.dumps(data, indent=2)
    blob.upload_from_string(json_data, content_type='application/json')
    
    return f'gs://{bucket_name}/{blob_path}' 