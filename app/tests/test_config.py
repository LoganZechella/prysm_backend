import pytest
import os
from pathlib import Path
import yaml
from app.config.manager import (
    AppConfig,
    ConfigurationError,
    GoogleCloudSettings,
    DatabaseSettings,
    RedisSettings,
    AuthSettings,
    APISettings,
    get_config
)

@pytest.fixture
def test_config_data():
    """Load test configuration data"""
    config_path = Path(__file__).parent / 'test_config.yaml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

@pytest.fixture
def mock_config_files(tmp_path, test_config_data):
    """Create temporary config files for testing"""
    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    
    # Write test config as base.yaml
    with open(config_dir / 'base.yaml', 'w') as f:
        yaml.dump(test_config_data, f)
    
    # Return the temp config directory
    return config_dir

def test_load_valid_config(test_config_data):
    """Test loading a valid configuration"""
    config = AppConfig(**test_config_data)
    
    assert config.env == test_config_data['env']
    assert config.debug == test_config_data['debug']
    assert config.log_level == test_config_data['log_level']
    assert config.gcp.project_id == test_config_data['gcp']['project_id']
    assert config.database.url.scheme == 'postgresql'
    assert config.redis.host == test_config_data['redis']['host']

def test_invalid_log_level(test_config_data):
    """Test validation of invalid log level"""
    invalid_config = test_config_data.copy()
    invalid_config['log_level'] = 'INVALID'
    
    with pytest.raises(ConfigurationError) as exc_info:
        AppConfig(**invalid_config)
    assert "Invalid log level" in str(exc_info.value)

def test_invalid_cors_origin(test_config_data):
    """Test validation of invalid CORS origin"""
    invalid_config = test_config_data.copy()
    invalid_config['api']['cors_origins'] = ['invalid-origin']
    
    with pytest.raises(ConfigurationError) as exc_info:
        AppConfig(**invalid_config)
    assert "Invalid CORS origin" in str(exc_info.value)

def test_invalid_database_pool_settings(test_config_data):
    """Test validation of invalid database pool settings"""
    invalid_config = test_config_data.copy()
    invalid_config['database']['pool_size'] = 10
    invalid_config['database']['max_overflow'] = 20  # Should be less than pool_size
    
    with pytest.raises(ConfigurationError) as exc_info:
        AppConfig(**invalid_config)
    assert "pool_size must be greater than max_overflow" in str(exc_info.value)

def test_invalid_redis_port(test_config_data):
    """Test validation of invalid Redis port"""
    invalid_config = test_config_data.copy()
    invalid_config['redis']['port'] = 70000  # Invalid port number
    
    with pytest.raises(ConfigurationError) as exc_info:
        AppConfig(**invalid_config)
    assert "Redis port must be between 1 and 65535" in str(exc_info.value)

def test_missing_jwt_key_in_production(test_config_data):
    """Test validation of missing JWT key in production"""
    os.environ['APP_ENV'] = 'production'
    invalid_config = test_config_data.copy()
    invalid_config['auth']['jwt_secret_key'] = ''
    
    with pytest.raises(ConfigurationError) as exc_info:
        AppConfig(**invalid_config)
    assert "JWT secret key must be set in non-development environments" in str(exc_info.value)

def test_environment_variable_override(test_config_data, monkeypatch):
    """Test that environment variables properly override config values"""
    # Create a minimal config with just the required fields
    config_data = {
        'env': 'test',
        'gcp': {
            'project_id': 'test-project',
            'region': 'us-central1',
            'zone': 'us-central1-a',
            'raw_data_bucket': 'test-bucket',
            'processed_data_bucket': 'test-bucket',
            'dataset_id': 'test'
        },
        'api_scopes': test_config_data['api_scopes'],
        'database': test_config_data['database'],
        'redis': test_config_data['redis'],
        'auth': test_config_data['auth'],
        'api': test_config_data['api']
    }
    
    # Set environment variables
    monkeypatch.setenv('PRYSM_GCP__PROJECT_ID', 'env-override-project')
    monkeypatch.setenv('PRYSM_DATABASE__POOL_SIZE', '50')
    monkeypatch.setenv('PRYSM_REDIS__PORT', '6380')
    
    # Create config with environment variables
    config = AppConfig.model_validate_with_config(config_data, config=AppConfig.model_config)
    
    # Environment variables should override the config values
    assert config.gcp.project_id == 'env-override-project'
    assert config.database.pool_size == 50
    assert config.redis.port == 6380

def test_api_scopes_validation(test_config_data):
    """Test validation of API scopes"""
    invalid_config = test_config_data.copy()
    invalid_config['api_scopes']['google'] = ['invalid-scope']
    
    with pytest.raises(ConfigurationError) as exc_info:
        AppConfig(**invalid_config)
    assert "Invalid scope format" in str(exc_info.value)

def test_required_fields(test_config_data):
    """Test that all required fields are validated"""
    invalid_config = test_config_data.copy()
    del invalid_config['gcp']['project_id']
    
    with pytest.raises(ConfigurationError) as exc_info:
        try:
            AppConfig.model_validate(invalid_config)
        except Exception as e:
            raise ConfigurationError(str(e))
    assert "Field required" in str(exc_info.value)

def test_get_config_caching(mock_config_files, monkeypatch):
    """Test that get_config caches the configuration"""
    # Set up test environment
    monkeypatch.setenv('APP_ENV', 'test')
    
    # Create a test config file
    test_config = {
        'env': 'test',
        'gcp': {
            'project_id': 'test-project',
            'region': 'us-central1',
            'zone': 'us-central1-a',
            'raw_data_bucket': 'test-bucket',
            'processed_data_bucket': 'test-bucket',
            'dataset_id': 'test'
        },
        'api_scopes': {
            'google': ['https://www.googleapis.com/auth/calendar.readonly'],
            'spotify': ['user-read-private'],
            'linkedin': ['r_liteprofile']
        },
        'database': {
            'url': 'postgresql://test:test@localhost:5432/test_db',
            'pool_size': 20,
            'max_overflow': 10,
            'pool_timeout': 30
        },
        'redis': {
            'host': 'localhost',
            'port': 6379,
            'db': 0
        },
        'auth': {
            'jwt_secret_key': 'test-key',
            'token_expiry': 3600,
            'refresh_token_expiry': 2592000
        },
        'api': {
            'cors_origins': ['http://localhost:3000'],
            'rate_limit': 100,
            'rate_limit_period': 60
        }
    }
    
    with open(mock_config_files / 'base.yaml', 'w') as f:
        yaml.dump(test_config, f)
    
    # Mock the config directory path
    def mock_path_join(self, *args):
        if args[0] == 'config':
            return mock_config_files
        return Path(os.path.join(str(self), *args))
    
    monkeypatch.setattr(Path, '__truediv__', mock_path_join)
    
    # First call should load config
    config1 = get_config()
    
    # Second call should return cached config
    config2 = get_config()
    
    assert config1 is config2  # Should be the same instance

if __name__ == '__main__':
    pytest.main([__file__, '-v']) 