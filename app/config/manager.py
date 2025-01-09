from typing import Dict, Any, Optional, List
from pydantic import PostgresDsn, RedisDsn, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
import os
from pathlib import Path
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    """Raised when there's an error in configuration loading or validation"""
    pass

class GoogleCloudSettings(BaseSettings):
    project_id: str
    region: str
    zone: str
    raw_data_bucket: str
    processed_data_bucket: str
    dataset_id: str

    model_config = SettingsConfigDict(env_prefix='PRYSM_GCP__')

    @field_validator('project_id')
    @classmethod
    def validate_project_id(cls, v: str) -> str:
        if not v:
            raise ConfigurationError("GCP project_id cannot be empty")
        return v

class APIScopes(BaseSettings):
    google: List[str]
    spotify: List[str]
    linkedin: List[str]

    model_config = SettingsConfigDict(env_prefix='PRYSM_API_SCOPES__')

    @field_validator('*')
    @classmethod
    def validate_scopes(cls, v: List[str]) -> List[str]:
        for scope in v:
            if not scope.startswith(('http://', 'https://')):
                if not any(scope.startswith(prefix) for prefix in ['user-', 'r_']):
                    raise ConfigurationError(f"Invalid scope format: {scope}")
        return v

class DatabaseSettings(BaseSettings):
    url: PostgresDsn
    pool_size: int = 20
    max_overflow: int = 10
    pool_timeout: int = 30

    model_config = SettingsConfigDict(env_prefix='PRYSM_DATABASE__')

    @field_validator('pool_size', 'max_overflow', 'pool_timeout')
    @classmethod
    def validate_positive(cls, v: int, field: str) -> int:
        if v <= 0:
            raise ConfigurationError(f"{field} must be positive")
        return v

    @model_validator(mode='after')
    def validate_pool_settings(self) -> 'DatabaseSettings':
        if self.pool_size < self.max_overflow:
            raise ConfigurationError("pool_size must be greater than max_overflow")
        return self

class RedisSettings(BaseSettings):
    host: str
    port: int = 6379
    db: int = 0

    model_config = SettingsConfigDict(env_prefix='PRYSM_REDIS__')

    @property
    def url(self) -> RedisDsn:
        return f"redis://{self.host}:{self.port}/{self.db}"

    @field_validator('port')
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ConfigurationError("Redis port must be between 1 and 65535")
        return v

    @field_validator('db')
    @classmethod
    def validate_db(cls, v: int) -> int:
        if v < 0:
            raise ConfigurationError("Redis db must be non-negative")
        return v

class AuthSettings(BaseSettings):
    jwt_secret_key: str
    token_expiry: int = 3600  # 1 hour
    refresh_token_expiry: int = 2592000  # 30 days

    model_config = SettingsConfigDict(env_prefix='PRYSM_AUTH__')

    @field_validator('jwt_secret_key')
    @classmethod
    def validate_jwt_key(cls, v: str) -> str:
        if not v and os.getenv('APP_ENV') != 'development':
            raise ConfigurationError("JWT secret key must be set in non-development environments")
        return v

    @field_validator('token_expiry', 'refresh_token_expiry')
    @classmethod
    def validate_expiry(cls, v: int) -> int:
        if v <= 0:
            raise ConfigurationError("Token expiry must be positive")
        return v

class APISettings(BaseSettings):
    cors_origins: List[str]
    rate_limit: int = 100
    rate_limit_period: int = 60

    model_config = SettingsConfigDict(env_prefix='PRYSM_API__')

    @field_validator('cors_origins')
    @classmethod
    def validate_cors_origins(cls, v: List[str]) -> List[str]:
        for origin in v:
            if not origin.startswith(('http://', 'https://')):
                raise ConfigurationError(f"Invalid CORS origin: {origin}")
        return v

    @field_validator('rate_limit', 'rate_limit_period')
    @classmethod
    def validate_rate_limit(cls, v: int) -> int:
        if v <= 0:
            raise ConfigurationError("Rate limit values must be positive")
        return v

class AppConfig(BaseSettings):
    env: str
    debug: bool = False
    log_level: str = "INFO"
    
    # Nested settings
    gcp: GoogleCloudSettings
    api_scopes: APIScopes
    database: DatabaseSettings
    redis: RedisSettings
    auth: AuthSettings
    api: APISettings
    
    model_config = SettingsConfigDict(
        env_prefix="PRYSM_",
        env_nested_delimiter="__",
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='allow'
    )
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ConfigurationError(f"Invalid log level. Must be one of: {', '.join(valid_levels)}")
        return v.upper()
    
    @classmethod
    def load(cls, env: str = None) -> 'AppConfig':
        """Load configuration for specified environment"""
        try:
            if not env:
                env = os.getenv('APP_ENV', 'development')
                
            config_dir = Path(__file__).parent.parent.parent / 'config'
            
            if not config_dir.exists():
                raise ConfigurationError(f"Configuration directory not found: {config_dir}")
            
            # Load base config
            base_config_path = config_dir / 'base.yaml'
            if not base_config_path.exists():
                raise ConfigurationError(f"Base configuration file not found: {base_config_path}")
                
            with open(base_config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # Load environment specific config
            env_file = config_dir / f'{env}.yaml'
            if env_file.exists():
                with open(env_file, 'r') as f:
                    env_config = yaml.safe_load(f)
                    config.update(env_config)
            else:
                logger.warning(f"Environment config file not found: {env_file}")
                
            # Load local overrides if they exist (gitignored)
            local_file = config_dir / 'local.yaml'
            if local_file.exists():
                with open(local_file, 'r') as f:
                    local_config = yaml.safe_load(f)
                    config.update(local_config)
            
            # Add environment name to config
            config['env'] = env
            
            try:
                return cls.model_validate(config)
            except Exception as e:
                raise ConfigurationError(f"Error validating configuration: {str(e)}")
                
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Error loading configuration: {str(e)}")

@lru_cache()
def get_config() -> AppConfig:
    """Get the application configuration.
    
    This function is cached to avoid loading the configuration multiple times.
    Use this function instead of creating AppConfig instances directly.
    """
    try:
        config = AppConfig.load()
        
        # Set up logging based on configuration
        logging.basicConfig(
            level=getattr(logging, config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        return config
    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        raise

# Only create the singleton instance when explicitly requested
config = None 