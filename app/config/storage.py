import os
import yaml
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class StorageConfig:
    """Configuration loader for storage settings"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize storage configuration"""
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config',
                'storage.yaml'
            )
        
        self.config = self._load_config(config_path)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading storage config: {str(e)}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            's3': {
                'bucket_name': 'prysm-events',
                'region': 'us-west-2',
                'lifecycle_rules': {
                    'raw_data_retention_days': 90,
                    'processed_data_retention_days': 30
                },
                'versioning': 'enabled'
            },
            'dynamodb': {
                'table_name': 'prysm-events',
                'region': 'us-west-2',
                'capacity': {
                    'read_units': 5,
                    'write_units': 5
                },
                'indexes': [
                    {
                        'name': 'start_datetime-index',
                        'key': 'start_datetime',
                        'type': 'hash'
                    },
                    {
                        'name': 'location-index',
                        'key': 'location_hash',
                        'type': 'hash'
                    }
                ],
                'stream': {
                    'enabled': True,
                    'view_type': 'NEW_AND_OLD_IMAGES'
                }
            },
            'manager': {
                'default_region': 'us-west-2',
                'fallback_strategy': 's3',
                'batch_size': 25
            }
        }
    
    @property
    def s3_config(self) -> Dict[str, Any]:
        """Get S3 configuration"""
        return self.config.get('s3', {})
    
    @property
    def dynamodb_config(self) -> Dict[str, Any]:
        """Get DynamoDB configuration"""
        return self.config.get('dynamodb', {})
    
    @property
    def manager_config(self) -> Dict[str, Any]:
        """Get storage manager configuration"""
        return self.config.get('manager', {})
    
    def get_s3_bucket_name(self) -> str:
        """Get S3 bucket name"""
        return self.s3_config.get('bucket_name', 'prysm-events')
    
    def get_s3_region(self) -> str:
        """Get S3 region"""
        return self.s3_config.get('region', 'us-west-2')
    
    def get_dynamodb_table_name(self) -> str:
        """Get DynamoDB table name"""
        return self.dynamodb_config.get('table_name', 'prysm-events')
    
    def get_dynamodb_region(self) -> str:
        """Get DynamoDB region"""
        return self.dynamodb_config.get('region', 'us-west-2')
    
    def get_default_region(self) -> str:
        """Get default region"""
        return self.manager_config.get('default_region', 'us-west-2')
    
    def get_fallback_strategy(self) -> str:
        """Get fallback strategy"""
        return self.manager_config.get('fallback_strategy', 's3')
    
    def get_batch_size(self) -> int:
        """Get batch size"""
        return self.manager_config.get('batch_size', 25)
    
    def get_s3_lifecycle_rules(self) -> Dict[str, int]:
        """Get S3 lifecycle rules"""
        return self.s3_config.get('lifecycle_rules', {
            'raw_data_retention_days': 90,
            'processed_data_retention_days': 30
        })
    
    def get_dynamodb_capacity(self) -> Dict[str, int]:
        """Get DynamoDB capacity settings"""
        return self.dynamodb_config.get('capacity', {
            'read_units': 5,
            'write_units': 5
        })
    
    def get_dynamodb_indexes(self) -> list:
        """Get DynamoDB indexes"""
        return self.dynamodb_config.get('indexes', [
            {
                'name': 'start_datetime-index',
                'key': 'start_datetime',
                'type': 'hash'
            },
            {
                'name': 'location-index',
                'key': 'location_hash',
                'type': 'hash'
            }
        ])
    
    def get_dynamodb_stream_config(self) -> Dict[str, Any]:
        """Get DynamoDB stream configuration"""
        return self.dynamodb_config.get('stream', {
            'enabled': True,
            'view_type': 'NEW_AND_OLD_IMAGES'
        }) 