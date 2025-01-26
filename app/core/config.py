"""Application configuration."""
from typing import List, Optional, Any, Dict
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

def clean_int_value(v: Any) -> int:
    """Clean integer values from environment variables."""
    if isinstance(v, str):
        # Remove any comments and whitespace
        v = v.split('#')[0].strip()
    return int(v)

class Settings(BaseSettings):
    # Application settings
    PROJECT_NAME: str = "Prysm Backend"
    API_V1_STR: str = "/api/v1"
    BACKEND_CORS_ORIGINS: List[str] = ["*"]
    APP_NAME: str = "Event Recommendation System"
    APP_VERSION: str = "1.0.0"
    ENVIRONMENT: str = "development"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = True
    RELOAD: bool = True
    
    # Redis settings
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # CORS settings
    ALLOWED_ORIGINS: str = "http://localhost:3000,http://localhost:8000"
    ALLOWED_METHODS: str = "GET,POST,PUT,DELETE,OPTIONS"
    ALLOWED_HEADERS: str = "*"
    
    # Cache settings
    CACHE_TTL: int = 3600
    CACHE_ENABLED: bool = True
    
    # Logging settings
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Security settings
    SESSION_SECRET_KEY: str = "your-secret-key"
    JWT_SECRET_KEY: str = "your-secret-key"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Database settings
    DATABASE_URL: str = "postgresql://logan@localhost:5432/prysm"
    POSTGRESQL_CONNECTION_URI: Optional[str] = None
    
    # OAuth settings
    SPOTIFY_CLIENT_ID: str
    SPOTIFY_CLIENT_SECRET: str
    SPOTIFY_REDIRECT_URI: str
    SPOTIFY_TEST_ACCESS_TOKEN: Optional[str] = None
    SPOTIFY_TEST_REFRESH_TOKEN: Optional[str] = None
    
    # Google OAuth settings
    GOOGLE_CLIENT_ID: str
    GOOGLE_CLIENT_SECRET: str
    GOOGLE_REDIRECT_URI: str
    GOOGLE_PROJECT_ID: str
    GOOGLE_TEST_ACCESS_TOKEN: Optional[str] = None
    GOOGLE_TEST_REFRESH_TOKEN: Optional[str] = None
    
    LINKEDIN_CLIENT_ID: Optional[str] = None
    LINKEDIN_CLIENT_SECRET: Optional[str] = None
    LINKEDIN_REDIRECT_URI: Optional[str] = None
    
    # Google Cloud settings
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None
    GCP_PROJECT_ID: Optional[str] = None
    GCP_CREDENTIALS_PATH: Optional[str] = None
    
    # NLP settings
    NLP_API_REGION: str = "global"
    NLP_API_TIMEOUT: int = 30
    NLP_REQUESTS_PER_MINUTE: int = 600
    NLP_BURST_SIZE: int = 100
    NLP_CACHE_TTL: int = 3600
    NLP_CACHE_MAX_SIZE: int = 1000
    NLP_MAX_RETRIES: int = 3
    NLP_INITIAL_RETRY_DELAY: float = 1.0
    NLP_MAX_RETRY_DELAY: float = 10.0
    
    # Storage settings
    GCS_RAW_BUCKET: Optional[str] = None
    GCS_PROCESSED_BUCKET: Optional[str] = None
    
    # Airflow settings
    AIRFLOW_HOME: Optional[str] = None
    AIRFLOW__CORE__FERNET_KEY: Optional[str] = None
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: Optional[str] = None
    AIRFLOW__SMTP__SMTP_HOST: Optional[str] = None
    AIRFLOW__SMTP__SMTP_USER: Optional[str] = None
    AIRFLOW__SMTP__SMTP_PASSWORD: Optional[str] = None
    AIRFLOW__SMTP__SMTP_PORT: int = 587
    AIRFLOW__SMTP__SMTP_MAIL_FROM: Optional[str] = None
    
    # Event API settings
    EVENTBRITE_API_KEY: Optional[str] = None
    EVENTBRITE_PRIVATE_TOKEN: Optional[str] = None
    META_ACCESS_TOKEN: Optional[str] = None
    META_APP_ID: Optional[str] = None
    META_APP_SECRET: Optional[str] = None
    SCRAPFLY_API_KEY: Optional[str] = None
    GOOGLE_MAPS_API_KEY: Optional[str] = None
    TICKETMASTER_API_KEY: Optional[str] = None
    TICKETMASTER_API_SECRET: Optional[str] = None
    
    # AWS settings
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_DEFAULT_REGION: str = "global"
    S3_BUCKET_PREFIX: Optional[str] = None
    DYNAMODB_EVENTS_TABLE: Optional[str] = None
    DYNAMODB_RAW_EVENTS_TABLE: Optional[str] = None
    
    # Meetup settings
    MEETUP_API_KEY: Optional[str] = None
    MEETUP_OAUTH_CLIENT_ID: Optional[str] = None
    MEETUP_OAUTH_CLIENT_SECRET: Optional[str] = None
    
    # Development settings
    NGROK_AUTH_TOKEN: Optional[str] = None
    
    # SuperTokens settings
    SUPERTOKENS_CONNECTION_URI: Optional[str] = None
    SUPERTOKENS_API_DOMAIN: Optional[str] = None
    SUPERTOKENS_WEBSITE_DOMAIN: Optional[str] = None
    SUPERTOKENS_API_BASE_PATH: Optional[str] = None
    SUPERTOKENS_WEBSITE_BASE_PATH: Optional[str] = None
    WEBSITE_DOMAIN: Optional[str] = None
    API_DOMAIN: Optional[str] = None

    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # Allow extra fields in the environment
    )

    # Validators for integer fields
    _clean_port = field_validator('PORT', 'REDIS_PORT', 'CACHE_TTL', 'ACCESS_TOKEN_EXPIRE_MINUTES',
                              'NLP_API_TIMEOUT', 'NLP_REQUESTS_PER_MINUTE', 'NLP_BURST_SIZE',
                              'NLP_CACHE_TTL', 'NLP_CACHE_MAX_SIZE', 'NLP_MAX_RETRIES',
                              'AIRFLOW__SMTP__SMTP_PORT', mode='before')(clean_int_value)

settings = Settings() 