"""
Google Cloud Platform configuration and utilities.
"""

import os
from typing import Optional, Dict, Any
from google.cloud import storage, language_v2
from google.oauth2 import service_account
from functools import lru_cache

from .settings import get_settings

settings = get_settings()

def get_credentials_path() -> str:
    """Get path to Google Cloud credentials file."""
    credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
    if not credentials_path or not os.path.exists(credentials_path):
        raise ValueError(
            "Google Cloud credentials file not found. "
            "Please set GOOGLE_APPLICATION_CREDENTIALS environment variable."
        )
    return credentials_path

def get_credentials(
    scopes: Optional[list] = None
) -> service_account.Credentials:
    """
    Get Google Cloud credentials from service account key file.
    
    Args:
        scopes: Optional list of scopes to request
        
    Returns:
        Google Cloud credentials
    """
    if not scopes:
        scopes = [
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/cloud-language'
        ]
        
    credentials_path = get_credentials_path()
    return service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=scopes
    )

@lru_cache()
def get_language_client() -> language_v2.LanguageServiceClient:
    """Get cached Google Cloud Natural Language client."""
    credentials = get_credentials()
    return language_v2.LanguageServiceClient(credentials=credentials)

@lru_cache()
def get_storage_client() -> storage.Client:
    """Get cached Google Cloud Storage client."""
    credentials = get_credentials()
    return storage.Client(
        credentials=credentials,
        project=settings.GCP_PROJECT_ID
    )

def get_nlp_config() -> Dict[str, Any]:
    """Get NLP service configuration."""
    return {
        'api_region': settings.NLP_API_REGION,
        'timeout': settings.NLP_API_TIMEOUT,
        'requests_per_minute': settings.NLP_REQUESTS_PER_MINUTE,
        'burst_size': settings.NLP_BURST_SIZE,
        'cache_ttl': settings.NLP_CACHE_TTL,
        'cache_max_size': settings.NLP_CACHE_MAX_SIZE,
        'max_retries': settings.NLP_MAX_RETRIES,
        'initial_retry_delay': settings.NLP_INITIAL_RETRY_DELAY,
        'max_retry_delay': settings.NLP_MAX_RETRY_DELAY
    }

def get_storage_config() -> Dict[str, Any]:
    """Get storage service configuration."""
    return {
        'raw_bucket': settings.GCS_RAW_BUCKET,
        'processed_bucket': settings.GCS_PROCESSED_BUCKET,
        'project_id': settings.GCP_PROJECT_ID
    } 