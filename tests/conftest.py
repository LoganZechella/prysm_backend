import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.database import Base, get_db
from app.main import app
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Add the app directory to the Python path for all tests
app_path = str(Path(__file__).parent.parent)
if app_path not in sys.path:
    sys.path.append(app_path)

# Create in-memory SQLite database for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def test_db():
    """Create a fresh test database for each test."""
    Base.metadata.create_all(bind=engine)
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def client(test_db):
    """Create a test client with a test database."""
    def override_get_db():
        try:
            yield test_db
        finally:
            test_db.close()
            
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)

@pytest.fixture(scope="function")
def mock_supertokens():
    """Mock SuperTokens for testing."""
    with patch("app.utils.auth.ep_sign_up") as mock_sign_up, \
         patch("app.utils.auth.ep_sign_in") as mock_sign_in, \
         patch("app.utils.auth.create_new_session") as mock_create_session:
        
        # Mock successful sign up
        mock_sign_up.return_value = MagicMock(
            user=MagicMock(user_id="test-user-123")
        )
        
        # Mock successful sign in
        mock_sign_in.return_value = MagicMock(
            user=MagicMock(user_id="test-user-123")
        )
        
        # Mock successful session creation
        mock_create_session.return_value = MagicMock(
            get_user_id=lambda: "test-user-123",
            get_access_token_payload=lambda: {}
        )
        
        yield {
            "sign_up": mock_sign_up,
            "sign_in": mock_sign_in,
            "create_session": mock_create_session
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