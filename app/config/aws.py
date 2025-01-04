import os
import yaml
import logging
from typing import Dict, Any, Optional
from string import Template

logger = logging.getLogger(__name__)

class AWSConfig:
    """Configuration loader for AWS settings"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize AWS configuration"""
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config',
                'aws.yaml'
            )
        
        self.config = self._load_config(config_path)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file with environment variable substitution"""
        try:
            with open(config_path, 'r') as f:
                # Load YAML content
                raw_content = f.read()
                # Substitute environment variables
                template = Template(raw_content)
                processed_content = template.safe_substitute(os.environ)
                # Parse YAML
                return yaml.safe_load(processed_content)['aws']
        except Exception as e:
            logger.error(f"Error loading AWS config: {str(e)}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default AWS configuration"""
        return {
            'credentials': {
                'access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
                'secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
                'session_token': os.getenv('AWS_SESSION_TOKEN')
            },
            'default_region': 'us-west-2',
            's3': {
                'bucket_prefix': 'prysm-events',
                'versioning': 'enabled',
                'lifecycle_rules': {
                    'raw_data_retention_days': 90,
                    'processed_data_retention_days': 30
                }
            },
            'dynamodb': {
                'endpoint': None,
                'table_prefix': 'prysm'
            }
        }
    
    def get_credentials(self) -> Dict[str, Optional[str]]:
        """Get AWS credentials"""
        creds = self.config.get('credentials', {})
        return {
            'access_key_id': creds.get('access_key_id'),
            'secret_access_key': creds.get('secret_access_key'),
            'session_token': creds.get('session_token')
        }
    
    def get_region(self) -> str:
        """Get AWS region"""
        return self.config.get('default_region', 'us-west-2')
    
    def get_s3_config(self) -> Dict[str, Any]:
        """Get S3 configuration"""
        return self.config.get('s3', {})
    
    def get_dynamodb_config(self) -> Dict[str, Any]:
        """Get DynamoDB configuration"""
        return self.config.get('dynamodb', {})
    
    def get_s3_bucket_name(self, suffix: str = '') -> str:
        """Get S3 bucket name with optional suffix"""
        prefix = os.getenv('S3_BUCKET_PREFIX', self.get_s3_config().get('bucket_prefix', 'prysm-events-test'))
        if suffix:
            return f"{prefix}-{suffix}"
        return prefix
    
    def get_dynamodb_table_name(self, suffix: str = '') -> str:
        """Get DynamoDB table name with optional suffix"""
        prefix = self.get_dynamodb_config().get('table_prefix', 'prysm')
        if suffix:
            return f"{prefix}-{suffix}"
        return prefix 