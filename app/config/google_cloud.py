"""
Google Cloud Platform configuration and settings.
"""

import os
from pathlib import Path
from typing import Optional

# GCP Credentials handling
GCP_CREDENTIALS_PATH = os.getenv('GCP_CREDENTIALS_PATH', str(Path(__file__).parent / 'credentials' / 'gcp-credentials.json'))

# API Settings
NLP_API_REGION = os.getenv('NLP_API_REGION', 'global')
NLP_API_TIMEOUT = int(os.getenv('NLP_API_TIMEOUT', '30'))  # seconds

# Rate Limiting
NLP_REQUESTS_PER_MINUTE = int(os.getenv('NLP_REQUESTS_PER_MINUTE', '600'))
NLP_BURST_SIZE = int(os.getenv('NLP_BURST_SIZE', '100'))

# Caching
NLP_CACHE_TTL = int(os.getenv('NLP_CACHE_TTL', '3600'))  # 1 hour in seconds
NLP_CACHE_MAX_SIZE = int(os.getenv('NLP_CACHE_MAX_SIZE', '1000'))

# Retry Configuration
NLP_MAX_RETRIES = int(os.getenv('NLP_MAX_RETRIES', '3'))
NLP_INITIAL_RETRY_DELAY = float(os.getenv('NLP_INITIAL_RETRY_DELAY', '1.0'))
NLP_MAX_RETRY_DELAY = float(os.getenv('NLP_MAX_RETRY_DELAY', '10.0'))

def validate_gcp_credentials() -> bool:
    """
    Validates that GCP credentials file exists and is readable.
    
    Returns:
        bool: True if credentials are valid, False otherwise
    """
    if not os.path.exists(GCP_CREDENTIALS_PATH):
        return False
        
    try:
        # Check if file is readable
        with open(GCP_CREDENTIALS_PATH, 'r') as f:
            return True
    except Exception:
        return False

def get_credentials_path() -> Optional[str]:
    """
    Gets the validated credentials path or None if invalid.
    
    Returns:
        Optional[str]: Path to credentials file if valid, None otherwise
    """
    return GCP_CREDENTIALS_PATH if validate_gcp_credentials() else None 