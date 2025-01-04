import pytest
import os
import yaml
from app.config.storage import StorageConfig

@pytest.fixture
def test_config_path(tmp_path):
    """Create a temporary config file"""
    config = {
        's3': {
            'bucket_name': 'test-bucket',
            'region': 'us-east-1',
            'lifecycle_rules': {
                'raw_data_retention_days': 60,
                'processed_data_retention_days': 15
            },
            'versioning': 'enabled'
        },
        'dynamodb': {
            'table_name': 'test-table',
            'region': 'us-east-1',
            'capacity': {
                'read_units': 10,
                'write_units': 10
            },
            'indexes': [
                {
                    'name': 'custom-index',
                    'key': 'custom_key',
                    'type': 'hash'
                }
            ],
            'stream': {
                'enabled': False,
                'view_type': 'KEYS_ONLY'
            }
        },
        'manager': {
            'default_region': 'us-east-1',
            'fallback_strategy': 'dynamodb',
            'batch_size': 50
        }
    }
    
    config_path = tmp_path / "test_storage.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(config, f)
    
    return str(config_path)

def test_load_config(test_config_path):
    """Test loading configuration from file"""
    config = StorageConfig(test_config_path)
    
    assert config.get_s3_bucket_name() == 'test-bucket'
    assert config.get_s3_region() == 'us-east-1'
    assert config.get_dynamodb_table_name() == 'test-table'
    assert config.get_dynamodb_region() == 'us-east-1'
    assert config.get_default_region() == 'us-east-1'
    assert config.get_fallback_strategy() == 'dynamodb'
    assert config.get_batch_size() == 50

def test_default_config():
    """Test default configuration values"""
    # Use non-existent path to trigger default config
    config = StorageConfig('/nonexistent/path.yaml')
    
    assert config.get_s3_bucket_name() == 'prysm-events'
    assert config.get_s3_region() == 'us-west-2'
    assert config.get_dynamodb_table_name() == 'prysm-events'
    assert config.get_dynamodb_region() == 'us-west-2'
    assert config.get_default_region() == 'us-west-2'
    assert config.get_fallback_strategy() == 's3'
    assert config.get_batch_size() == 25

def test_lifecycle_rules(test_config_path):
    """Test S3 lifecycle rules configuration"""
    config = StorageConfig(test_config_path)
    rules = config.get_s3_lifecycle_rules()
    
    assert rules['raw_data_retention_days'] == 60
    assert rules['processed_data_retention_days'] == 15

def test_dynamodb_capacity(test_config_path):
    """Test DynamoDB capacity configuration"""
    config = StorageConfig(test_config_path)
    capacity = config.get_dynamodb_capacity()
    
    assert capacity['read_units'] == 10
    assert capacity['write_units'] == 10

def test_dynamodb_indexes(test_config_path):
    """Test DynamoDB indexes configuration"""
    config = StorageConfig(test_config_path)
    indexes = config.get_dynamodb_indexes()
    
    assert len(indexes) == 1
    assert indexes[0]['name'] == 'custom-index'
    assert indexes[0]['key'] == 'custom_key'
    assert indexes[0]['type'] == 'hash'

def test_dynamodb_stream_config(test_config_path):
    """Test DynamoDB stream configuration"""
    config = StorageConfig(test_config_path)
    stream_config = config.get_dynamodb_stream_config()
    
    assert stream_config['enabled'] is False
    assert stream_config['view_type'] == 'KEYS_ONLY'

def test_config_properties(test_config_path):
    """Test configuration property access"""
    config = StorageConfig(test_config_path)
    
    assert config.s3_config['bucket_name'] == 'test-bucket'
    assert config.dynamodb_config['table_name'] == 'test-table'
    assert config.manager_config['batch_size'] == 50

def test_missing_values():
    """Test handling of missing configuration values"""
    config = StorageConfig()
    
    # Create empty config file
    empty_config = {}
    config.config = empty_config
    
    # Test default values
    assert config.get_s3_bucket_name() == 'prysm-events'
    assert config.get_s3_region() == 'us-west-2'
    assert config.get_dynamodb_table_name() == 'prysm-events'
    assert config.get_dynamodb_region() == 'us-west-2'
    assert config.get_default_region() == 'us-west-2'
    assert config.get_fallback_strategy() == 's3'
    assert config.get_batch_size() == 25

def test_invalid_config():
    """Test handling of invalid configuration"""
    config = StorageConfig()
    
    # Set invalid config
    config.config = None
    
    # Test default values
    assert config.get_s3_bucket_name() == 'prysm-events'
    assert config.get_s3_region() == 'us-west-2'
    assert config.get_dynamodb_table_name() == 'prysm-events'
    assert config.get_dynamodb_region() == 'us-west-2'
    assert config.get_default_region() == 'us-west-2'
    assert config.get_fallback_strategy() == 's3'
    assert config.get_batch_size() == 25 