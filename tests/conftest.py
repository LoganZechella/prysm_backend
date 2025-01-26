"""Test configuration and fixtures."""

import os
import pytest
from app.database import Base
from app.services.event_collection import EventCollectionService
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://logan@localhost:5432/test_db"

# Test Scrapfly API key
TEST_SCRAPFLY_API_KEY = "test_key"

# Configure test database
engine = create_async_engine(TEST_DATABASE_URL)
TestingSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

# Create tables
async def init_test_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

# Dependency override
async def override_get_db():
    async with TestingSessionLocal() as session:
        yield session

# Event collection service fixture
@pytest.fixture
def event_collection_service():
    """Create event collection service for testing."""
    return EventCollectionService(scrapfly_api_key=TEST_SCRAPFLY_API_KEY) 