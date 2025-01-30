"""Application configuration settings."""

from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import model_validator

class Settings(BaseSettings):
    """Application settings."""
    
    # Application
    APP_NAME: str = "Event Recommendation System"
    APP_VERSION: str = "1.0.0"
    ENVIRONMENT: str = "development"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = True
    RELOAD: bool = True
    
    # Database
    DATABASE_URL: str = "postgresql://logan@localhost:5432/prysm"
    DATABASE_POOL_SIZE: int = 5
    DATABASE_MAX_OVERFLOW: int = 10
    POSTGRESQL_CONNECTION_URI: Optional[str] = None
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # API
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Prysm Backend"
    API_DOMAIN: str = "http://localhost:8000"
    WEBSITE_DOMAIN: str = "http://localhost:3001"
    
    # Security
    SECRET_KEY: str = "your-secret-key"  # Change in production
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8  # 8 days
    JWT_SECRET_KEY: str = "your-secret-key-here"
    JWT_ALGORITHM: str = "HS256"
    SESSION_SECRET_KEY: str = "VSlaDxSkBvJB8sKt"
    
    # CORS
    ALLOWED_ORIGINS: str = "http://localhost:3000,http://localhost:8000"
    ALLOWED_METHODS: str = "GET,POST,PUT,DELETE,OPTIONS"
    ALLOWED_HEADERS: str = "*"
    
    # Cache
    CACHE_ENABLED: bool = True
    CACHE_TTL: int = 3600  # 1 hour
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # OAuth2 - Spotify
    SPOTIFY_CLIENT_ID: Optional[str] = None
    SPOTIFY_CLIENT_SECRET: Optional[str] = None
    SPOTIFY_REDIRECT_URI: Optional[str] = None
    SPOTIFY_TEST_ACCESS_TOKEN: Optional[str] = None
    SPOTIFY_TEST_REFRESH_TOKEN: Optional[str] = None
    
    # OAuth2 - Google
    GOOGLE_CLIENT_ID: Optional[str] = None
    GOOGLE_CLIENT_SECRET: Optional[str] = None
    GOOGLE_REDIRECT_URI: Optional[str] = None
    GOOGLE_PROJECT_ID: Optional[str] = None
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None
    
    # OAuth2 - LinkedIn
    LINKEDIN_CLIENT_ID: Optional[str] = None
    LINKEDIN_CLIENT_SECRET: Optional[str] = None
    LINKEDIN_REDIRECT_URI: Optional[str] = None
    
    # GCP
    GCP_PROJECT_ID: Optional[str] = None
    GCP_CREDENTIALS_PATH: Optional[str] = None
    
    # NLP Service
    NLP_API_REGION: str = "global"
    NLP_API_TIMEOUT: int = 30
    NLP_REQUESTS_PER_MINUTE: int = 600
    NLP_BURST_SIZE: int = 100
    NLP_CACHE_TTL: int = 3600
    NLP_CACHE_MAX_SIZE: int = 1000
    NLP_MAX_RETRIES: int = 3
    NLP_INITIAL_RETRY_DELAY: float = 1.0
    NLP_MAX_RETRY_DELAY: float = 10.0
    
    # Google Cloud Storage
    GCS_RAW_BUCKET: Optional[str] = None
    GCS_PROCESSED_BUCKET: Optional[str] = None
    
    # Airflow
    AIRFLOW_HOME: Optional[str] = None
    AIRFLOW__CORE__FERNET_KEY: Optional[str] = None
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: Optional[str] = None
    AIRFLOW__SMTP__SMTP_HOST: Optional[str] = None
    AIRFLOW__SMTP__SMTP_USER: Optional[str] = None
    AIRFLOW__SMTP__SMTP_PASSWORD: Optional[str] = None
    AIRFLOW__SMTP__SMTP_PORT: int = 587
    AIRFLOW__SMTP__SMTP_MAIL_FROM: Optional[str] = None
    
    # Event Sources
    EVENTBRITE_PRIVATE_TOKEN: Optional[str] = None
    META_ACCESS_TOKEN: Optional[str] = None
    META_APP_ID: Optional[str] = None
    META_APP_SECRET: Optional[str] = None
    FACEBOOK_USER_ID: Optional[str] = None
    FACEBOOK_XS_TOKEN: Optional[str] = None
    SCRAPFLY_API_KEY: Optional[str] = None
    GOOGLE_MAPS_API_KEY: Optional[str] = None
    TICKETMASTER_API_KEY: Optional[str] = None
    TICKETMASTER_API_SECRET: Optional[str] = None
    
    # AWS
    AWS_REGION: str = "us-east-2"
    AWS_SAGEMAKER_ROLE_ARN: Optional[str] = None
    AWS_FEATURE_STORE_ROLE_ARN: Optional[str] = None
    AWS_S3_BUCKET: Optional[str] = None
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    S3_BUCKET_PREFIX: str = "prysm-events"
    DYNAMODB_EVENTS_TABLE: str = "prysm-events"
    DYNAMODB_RAW_EVENTS_TABLE: str = "prysm-raw-events"
    
    # Meetup
    MEETUP_OAUTH_CLIENT_ID: Optional[str] = None
    MEETUP_OAUTH_CLIENT_SECRET: Optional[str] = None
    
    # Ngrok
    NGROK_AUTH_TOKEN: Optional[str] = None
    
    # SuperTokens
    SUPERTOKENS_CONNECTION_URI: str = "postgresql://logan@localhost:5432/prysm"
    SUPERTOKENS_API_DOMAIN: str = "http://localhost:8000"
    SUPERTOKENS_WEBSITE_DOMAIN: str = "http://localhost:3001"
    SUPERTOKENS_API_BASE_PATH: str = "/api/auth"
    SUPERTOKENS_WEBSITE_BASE_PATH: str = "/auth"
    
    # ML Pipeline
    MODEL_CACHE_DIR: str = "./models"
    FEATURE_STORE_PATH: str = "./features"
    
    # Monitoring
    SENTRY_DSN: Optional[str] = None
    
    # Performance
    MAX_WORKERS: int = 4
    RATE_LIMIT: int = 100  # requests per minute
    
    model_config = {
        "env_file": ".env",
        "case_sensitive": True,
        "extra": "allow"  # Allow extra fields to support future additions
    }
    
    @model_validator(mode='before')
    @classmethod
    def validate_database_url(cls, values):
        """Validate database URL configuration."""
        if values.get("POSTGRESQL_CONNECTION_URI"):
            values["DATABASE_URL"] = values["POSTGRESQL_CONNECTION_URI"]
        return values

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings() 