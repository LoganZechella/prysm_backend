import pytest
import asyncio
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text
from app.main import app
from app.database import get_db
from tests.integration.infrastructure.database import TestDatabase
from tests.integration.mocks.mock_services import MockEventSource, MockRecommendationService, MockLocationService
from tests.integration.config.test_env import setup_test_env
import os

# Set up test environment
test_env = setup_test_env()

@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Set up test database."""
    # Create test database if it doesn't exist
    from sqlalchemy_utils import create_database, database_exists
    
    database_url = test_env['DATABASE_URL']
    if not database_exists(database_url):
        create_database(database_url)
    
    # Run migrations
    from alembic.config import Config
    from alembic import command
    
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "head")
    
    yield
    
    # Clean up - could drop test database here if needed
    # drop_database(database_url)

@pytest.fixture
def event_loop():
    """Create event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def test_db() -> Session:
    """Fixture providing test database session."""
    db = next(get_db())
    try:
        yield db
    finally:
        db.close()

@pytest.fixture
def test_client(test_db):
    """Fixture providing FastAPI test client."""
    # Override the dependency
    app.dependency_overrides[get_db] = lambda: test_db
    
    with TestClient(app) as client:
        yield client
    
    # Clear the override
    app.dependency_overrides.clear()

@pytest.fixture
def mock_services():
    """Fixture providing mock services."""
    return {
        'event_source': MockEventSource(),
        'recommendations': MockRecommendationService(),
        'location': MockLocationService()
    }

@pytest.fixture(autouse=True)
def setup_test_env(test_db):
    """Set up test environment and clean up after each test."""
    # Set up - can add any necessary test data setup here
    yield
    
    # Clean up - remove test data after each test
    inspector = inspect(test_db.get_bind())
    for table in reversed(inspector.get_table_names()):
        test_db.execute(text(f'TRUNCATE TABLE {table} CASCADE'))
    test_db.commit() 