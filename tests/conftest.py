import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.database import Base, get_db
from app.main import app
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path
import logging
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Add the app directory to the Python path for all tests
app_path = str(Path(__file__).parent.parent)
if app_path not in sys.path:
    sys.path.append(app_path)

# Create PostgreSQL test database
SQLALCHEMY_DATABASE_URL = "postgresql://localhost/prysm_test"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="session", autouse=True)
def mock_gcs():
    """Mock Google Cloud Storage client."""
    with patch('google.cloud.storage.Client') as mock_client:
        # Mock bucket and blob
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        # Setup mock returns
        mock_client.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.download_as_string.return_value = b'{"test": "data"}'
        
        yield mock_client

@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Create test database if it doesn't exist."""
    default_engine = create_engine("postgresql://localhost/postgres")
    with default_engine.connect() as conn:
        conn.execute(text("COMMIT"))  # Close any open transactions
        conn.execute(text("DROP DATABASE IF EXISTS prysm_test"))
        conn.execute(text("CREATE DATABASE prysm_test"))
    default_engine.dispose()

@pytest.fixture(scope="function")
def test_db():
    """Create a fresh test database for each test."""
    # Import all models to ensure they're registered with SQLAlchemy
    from app.models.preferences import UserPreferences
    from app.models.oauth import OAuthToken
    
    # Create a new session
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)
    
    try:
        # Drop all tables first to ensure a clean state
        Base.metadata.drop_all(bind=connection)
        
        # Create all tables
        Base.metadata.create_all(bind=connection)
        logger.debug("Successfully created database tables")
        
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()

@pytest.fixture(scope="function")
def client(test_db):
    """Create a test client with a test database."""
    def override_get_db():
        try:
            yield test_db
        finally:
            pass  # Don't close the session here, it's handled by the test_db fixture
            
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)

@pytest.fixture(scope="function")
def mock_supertokens():
    """Mock SuperTokens for testing."""
    with patch("app.utils.auth.get_session") as mock_get_session, \
         patch("app.utils.auth.create_new_session") as mock_create_session:
        
        # Mock session container
        mock_session = MagicMock()
        mock_session.get_user_id.return_value = "test-user-123"
        mock_session.get_access_token_payload.return_value = {}
        mock_session.merge_into_access_token_payload = MagicMock()
        mock_session.get_handle.return_value = "test-session-handle"
        
        # Make async-compatible mocks
        async def async_get_session(*args, **kwargs):
            return mock_session
            
        async def async_create_session(*args, **kwargs):
            return mock_session
            
        mock_get_session.side_effect = async_get_session
        mock_create_session.side_effect = async_create_session
        
        return {
            "get_session": mock_get_session,
            "create_session": mock_create_session,
            "mock_session": mock_session
        }

@pytest.fixture(scope="function")
def mock_session():
    """Create a mock session for testing."""
    return {
        "user_id": "test-user-123",
        "access_token_payload": {}
    }

@pytest.fixture(scope="session")
def test_events():
    """Create test events for recommendation testing."""
    from app.utils.test_data_generator import TestDataGenerator
    generator = TestDataGenerator()
    return generator.generate_events(num_events=50)

@pytest.fixture(scope="session")
def recommendation_engine():
    """Create a recommendation engine instance for testing."""
    from app.utils.recommendation import RecommendationEngine
    return RecommendationEngine()

@pytest.fixture(scope="session")
def data_quality_checker():
    """Create a data quality checker instance for testing."""
    from app.utils.data_quality import DataQualityChecker
    return DataQualityChecker()

@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    with patch.dict(os.environ, {
        "SPOTIFY_CLIENT_ID": "test_client_id",
        "SPOTIFY_CLIENT_SECRET": "test_client_secret",
        "SPOTIFY_REDIRECT_URI": "http://localhost:8000/api/auth/spotify/callback",
        "GOOGLE_CLIENT_ID": "test_google_client_id",
        "GOOGLE_CLIENT_SECRET": "test_google_client_secret",
        "GOOGLE_REDIRECT_URI": "http://localhost:8000/api/auth/google/callback",
        "LINKEDIN_CLIENT_ID": "test_linkedin_client_id",
        "LINKEDIN_CLIENT_SECRET": "test_linkedin_client_secret",
        "LINKEDIN_REDIRECT_URI": "http://localhost:8000/api/auth/linkedin/callback",
        "DATABASE_URL": "postgresql://localhost/prysm_test",
        "FRONTEND_URL": "http://localhost:3001",
        "API_BASE_URL": "http://localhost:8000"
    }):
        yield 