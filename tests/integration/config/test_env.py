"""Test environment configuration."""
import os
from typing import Dict, Any
from app.utils.logging import setup_logger
from pathlib import Path

logger = setup_logger(__name__)

# Default test environment variables
test_env = {
    # Database
    'DATABASE_URL': 'postgresql://logan@localhost:5432/prysm_test',
    
    # Spotify
    'SPOTIFY_CLIENT_ID': os.getenv('SPOTIFY_CLIENT_ID'),
    'SPOTIFY_CLIENT_SECRET': os.getenv('SPOTIFY_CLIENT_SECRET'),
    'SPOTIFY_REDIRECT_URI': os.getenv('SPOTIFY_REDIRECT_URI'),
    'SPOTIFY_TEST_ACCESS_TOKEN': os.getenv('SPOTIFY_TEST_ACCESS_TOKEN'),
    'SPOTIFY_TEST_REFRESH_TOKEN': os.getenv('SPOTIFY_TEST_REFRESH_TOKEN'),
    
    # Google
    'GOOGLE_CLIENT_ID': os.getenv('GOOGLE_CLIENT_ID'),
    'GOOGLE_CLIENT_SECRET': os.getenv('GOOGLE_CLIENT_SECRET'),
    'GOOGLE_REDIRECT_URI': os.getenv('GOOGLE_REDIRECT_URI'),
    
    # LinkedIn
    'LINKEDIN_CLIENT_ID': os.getenv('LINKEDIN_CLIENT_ID'),
    'LINKEDIN_CLIENT_SECRET': os.getenv('LINKEDIN_CLIENT_SECRET'),
    'LINKEDIN_REDIRECT_URI': os.getenv('LINKEDIN_REDIRECT_URI'),
    
    # JWT
    'JWT_SECRET_KEY': 'test-secret-key',
    'JWT_ALGORITHM': 'HS256',
    'ACCESS_TOKEN_EXPIRE_MINUTES': '30',
    
    # App
    'APP_ENV': 'test',
    'DEBUG': 'true',
    'LOG_LEVEL': 'DEBUG'
}

def setup_test_env() -> Dict[str, Any]:
    """Set up test environment variables."""
    print("\nSetting up test environment...")
    print(f"Current working directory: {os.getcwd()}")
    
    # First, check if we have a .env file and load it
    try:
        from dotenv import load_dotenv
        env_path = Path('.env.test')
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Loaded test environment from {env_path}")
        else:
            env_path = Path('.env')
            if env_path.exists():
                load_dotenv(env_path)
                print(f"Loaded environment from {env_path}")
            else:
                print("No .env or .env.test file found")
                print("Available files in current directory:", os.listdir('.'))
    except ImportError:
        print("python-dotenv not installed, skipping .env file load")
    
    # Update test_env with any values from environment
    for key in test_env:
        env_value = os.getenv(key)
        if env_value is not None:
            test_env[key] = env_value
    
    # Print all environment variables (with sensitive data masked)
    print("\nEnvironment variables being set:")
    for key, value in test_env.items():
        if value is None:
            print(f"{key}: Not set")
        elif any(sensitive in key.lower() for sensitive in ['secret', 'token', 'key']):
            masked_value = f"{'*' * 10}{value[-5:] if value else 'None'}"
            print(f"{key}: {masked_value}")
        else:
            print(f"{key}: {value}")
    
    # Set environment variables
    for key, value in test_env.items():
        if value is not None:
            os.environ[key] = str(value)
            
    # Verify critical variables
    critical_vars = [
        'SPOTIFY_CLIENT_ID',
        'SPOTIFY_CLIENT_SECRET',
        'SPOTIFY_REDIRECT_URI',
        'SPOTIFY_TEST_ACCESS_TOKEN',
        'SPOTIFY_TEST_REFRESH_TOKEN'
    ]
    
    missing_critical = [var for var in critical_vars if not os.getenv(var)]
    if missing_critical:
        print(f"\nWARNING: Missing critical environment variables: {', '.join(missing_critical)}")
        print("These variables are required for Spotify integration tests to work")
    else:
        print("\nAll critical environment variables are set")
    
    return test_env 