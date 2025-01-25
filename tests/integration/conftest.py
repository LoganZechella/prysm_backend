"""Test configuration for integration tests."""
import pytest
import asyncio
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text, create_engine
from app.main import app
from app.database import get_db, SessionLocal
from tests.integration.infrastructure.database import TestDatabase
from tests.integration.mocks.mock_services import MockEventSource, MockRecommendationService, MockLocationService
from tests.integration.config.test_env import setup_test_env, test_env
import os
from sqlalchemy_utils import create_database, database_exists, drop_database

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    env_vars = setup_test_env()
    for key, value in env_vars.items():
        if value is not None:
            os.environ[key] = str(value)
    yield
    # Clean up environment if needed
    pass

@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Set up test database."""
    database_url = test_env['DATABASE_URL']
    print(f"\nSetting up test database at {database_url}")

    # Create a connection to the default postgres database
    postgres_url = database_url.rsplit('/', 1)[0] + '/postgres'
    engine = create_engine(postgres_url)
    
    # Drop and recreate the test database
    with engine.connect() as conn:
        conn.execute(text("COMMIT"))  # Close any open transactions
        
        # Drop the test database if it exists
        if database_exists(database_url):
            print("Dropping existing database...")
            drop_database(database_url)
        
        print("Creating new database...")
        create_database(database_url)
    
    # Run migrations on the test database
    from alembic import command
    from alembic.config import Config
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "head")
    
    yield
    
    # Clean up after tests
    print("\nCleaning up test database...")
    drop_database(database_url)

@pytest.fixture
def event_loop():
    """Create event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def test_db() -> Session:
    """Create a test database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@pytest.fixture
def test_client(test_db: Session) -> TestClient:
    """Create a test client."""
    def override_get_db():
        try:
            yield test_db
        finally:
            test_db.close()
            
    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)

@pytest.fixture
def mock_services():
    """Fixture providing mock services."""
    return {
        'event_source': MockEventSource(),
        'recommendations': MockRecommendationService(),
        'location': MockLocationService()
    }

@pytest.fixture(autouse=True)
def cleanup_test_data(test_db):
    """Clean up test data after each test."""
    yield
    inspector = inspect(test_db.get_bind())
    for table in reversed(inspector.get_table_names()):
        test_db.execute(text(f'TRUNCATE TABLE {table} CASCADE'))
    test_db.commit()

@pytest.fixture(autouse=True)
def setup_env():
    """Set up test environment variables."""
    setup_test_env()
    yield

@pytest.fixture(autouse=True)
def cleanup_env():
    """Clean up test environment variables after each test."""
    yield
    # Clean up environment if needed
    pass 