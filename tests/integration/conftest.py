import pytest
import asyncio
from fastapi.testclient import TestClient
from app.main import app
from tests.integration.infrastructure.database import TestDatabase
from tests.integration.mocks.mock_services import MockEventSource, MockRecommendationService, MockLocationService
import os

# Set test environment
os.environ["APP_ENV"] = "test"

@pytest.fixture
def event_loop():
    """Create event loop for each test."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def test_db():
    """Fixture providing test database connection"""
    from app.db.session import get_db
    
    async for db in get_db():
        yield db

@pytest.fixture
async def test_client(test_db):
    """Fixture providing FastAPI test client"""
    from fastapi.testclient import TestClient
    from app.main import app

    # Ensure database is ready
    await anext(test_db)
    
    # Create test client
    client = TestClient(app)
    return client

@pytest.fixture
def mock_services():
    """Fixture providing mock services."""
    return {
        'event_source': MockEventSource(),
        'recommendations': MockRecommendationService(),
        'location': MockLocationService()
    } 