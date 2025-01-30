"""Tests for the trait extractor service."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, AsyncMock
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
import asyncio
import uuid
import pytz
import json

from app.database.base import Base
from app.models.traits import Traits
from app.services.trait_extractor import TraitExtractor
from app.core.cache import redis_client

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def engine():
    """Create a test database engine."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture
async def db_session(engine):
    """Create a test database session."""
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session:
        yield session
        await session.rollback()

@pytest.fixture
def mock_redis():
    """Mock Redis client."""
    with patch('app.core.cache.redis_client') as mock:
        # Mock the Redis get and setex methods
        mock.get.return_value = None
        yield mock

@pytest.fixture
def mock_spotify_service():
    """Mock Spotify service for testing."""
    service = MagicMock()
    service.extract_music_traits.return_value = {
        "genres": {
            "rock": 0.8,
            "indie": 0.6,
            "electronic": 0.4
        },
        "artists": {
            "short_term": ["Artist1", "Artist2"],
            "medium_term": ["Artist3", "Artist4"],
            "long_term": ["Artist5", "Artist6"]
        },
        "tracks": {
            "short_term": ["Track1", "Track2"],
            "medium_term": ["Track3", "Track4"],
            "long_term": ["Track5", "Track6"]
        }
    }
    service.extract_social_traits.return_value = {
        "playlist_count": 10,
        "collaborative_playlists": 2,
        "public_playlists": 5,
        "following_count": 100,
        "social_score": 0.75
    }
    service.extract_behavior_traits.return_value = {
        "listening_times": {
            "morning": 0.3,
            "afternoon": 0.5,
            "evening": 0.2
        },
        "discovery_ratio": 0.4,
        "engagement_level": 0.8
    }
    return service

@pytest.fixture
def mock_google_service():
    """Mock Google service for testing."""
    service = MagicMock()
    service.extract_professional_traits.return_value = {
        "urls": ["https://github.com/user", "https://linkedin.com/user"],
        "skills": ["Python", "Machine Learning", "Data Science"],
        "interests": ["Technology", "AI", "Music"],
        "locations": ["San Francisco, CA"],
        "occupations": ["Software Engineer"],
        "organizations": ["Tech Corp"],
        "last_updated": datetime.utcnow().isoformat()
    }
    return service

@pytest.fixture
def mock_linkedin_service():
    """Mock LinkedIn service for testing."""
    service = MagicMock()
    service.extract_professional_traits.return_value = {
        "name": "Test User",
        "email": "test@example.com",
        "picture_url": "https://example.com/pic.jpg",
        "locale": "en_US",
        "language": "en",
        "last_updated": datetime.utcnow().isoformat(),
        "next_update": (datetime.utcnow() + timedelta(days=7)).isoformat()
    }
    return service

@pytest.fixture
async def trait_extractor(db_session, mock_redis):
    """Create a TraitExtractor instance with mocked services."""
    # Mock Spotify service
    spotify_service = AsyncMock()
    spotify_service.get_music_traits.return_value = {
        "top_genres": ["rock", "indie", "electronic"],
        "favorite_artists": ["Artist1", "Artist2"],
        "listening_time": 3600
    }
    spotify_service.get_social_traits.return_value = {
        "playlist_count": 10,
        "follower_count": 100,
        "following_count": 50
    }
    spotify_service.get_behavior_traits.return_value = {
        "discovery_rate": 0.7,
        "repeat_rate": 0.3,
        "listening_schedule": ["morning", "evening"]
    }
    
    # Mock Spotify API methods to return values instead of coroutines
    spotify_service.get_top_tracks.return_value = {
        'items': [
            {
                'id': 'track1',
                'name': 'Track 1',
                'artists': [
                    {
                        'id': 'artist1',
                        'name': 'Artist 1'
                    }
                ],
                'popularity': 80
            }
        ]
    }
    spotify_service.get_top_artists.return_value = {
        'items': [
            {
                'id': 'artist1',
                'name': 'Artist 1',
                'genres': ['rock', 'indie'],
                'popularity': 75
            }
        ]
    }

    # Mock Google service
    google_service = AsyncMock()
    google_service.get_professional_traits.return_value = {
        "skills": ["Python", "Data Science"],
        "interests": ["Technology", "Music"],
        "education": ["Computer Science"]
    }

    # Mock LinkedIn service
    linkedin_service = AsyncMock()
    linkedin_service.get_professional_traits.return_value = {
        "job_titles": ["Software Engineer"],
        "industries": ["Technology"],
        "experience_years": 5
    }

    extractor = TraitExtractor(
        db=db_session,
        spotify_service=spotify_service,
        google_service=google_service,
        linkedin_service=linkedin_service
    )
    
    return extractor

@pytest.mark.asyncio
async def test_update_user_traits(trait_extractor, db_session):
    """Test updating user traits."""
    user_id = "test-user-1"
    
    # Update traits
    traits = await trait_extractor.update_user_traits(user_id)
    
    # Verify traits were created
    assert traits is not None
    assert traits.user_id == user_id
    assert traits.music_traits == trait_extractor.spotify_service.get_music_traits.return_value
    assert traits.social_traits == trait_extractor.spotify_service.get_social_traits.return_value
    assert traits.behavior_traits == trait_extractor.spotify_service.get_behavior_traits.return_value
    
    # Verify professional traits
    expected_professional_traits = {
        "google": {
            "skills": ["Python", "Data Science"],
            "interests": ["Technology", "Music"],
            "education": ["Computer Science"]
        },
        "linkedin": {
            "job_titles": ["Software Engineer"],
            "industries": ["Technology"],
            "experience_years": 5
        }
    }
    assert traits.professional_traits == expected_professional_traits

@pytest.mark.asyncio
async def test_get_user_traits(trait_extractor, db_session):
    """Test retrieving user traits."""
    user_id = "test-user-1"
    
    # First update traits
    await trait_extractor.update_user_traits(user_id)

    # Then retrieve them using SQLAlchemy 2.0 style
    stmt = select(Traits).where(Traits.user_id == user_id)
    result = await db_session.execute(stmt)
    traits = result.scalar_one()

    traits_dict = await trait_extractor.get_user_traits(user_id)
    assert traits_dict is not None
    assert traits_dict["music_traits"] == trait_extractor.spotify_service.get_music_traits.return_value
    assert traits_dict["social_traits"] == trait_extractor.spotify_service.get_social_traits.return_value
    assert traits_dict["behavior_traits"] == trait_extractor.spotify_service.get_behavior_traits.return_value

@pytest.mark.asyncio
async def test_trait_caching(trait_extractor, db_session, mock_redis):
    """Test that traits are properly cached."""
    user_id = "test-user-1"
    cached_music_traits = {
        "genres": {
            "rock": 0.8,
            "indie": 0.6
        },
        "artists": {
            "short_term": [{"id": "artist1", "name": "Artist 1", "genres": ["rock"], "popularity": 75}],
            "medium_term": [{"id": "artist1", "name": "Artist 1", "genres": ["rock"], "popularity": 75}],
            "long_term": [{"id": "artist1", "name": "Artist 1", "genres": ["rock"], "popularity": 75}]
        },
        "tracks": {
            "short_term": [{"id": "track1", "name": "Track 1", "artists": [{"id": "artist1", "name": "Artist 1"}], "popularity": 80}],
            "medium_term": [{"id": "track1", "name": "Track 1", "artists": [{"id": "artist1", "name": "Artist 1"}], "popularity": 80}],
            "long_term": [{"id": "track1", "name": "Track 1", "artists": [{"id": "artist1", "name": "Artist 1"}], "popularity": 80}]
        }
    }

    # Mock Redis to return cached data on second call
    mock_redis.get.side_effect = [
        None,  # First call returns None (cache miss)
        json.dumps(cached_music_traits)  # Second call returns cached data
    ]

    # First call should hit the service
    traits1 = await trait_extractor.extract_music_traits(user_id)
    assert trait_extractor.spotify_service.get_top_tracks.await_count == 3  # short, medium, long term
    assert trait_extractor.spotify_service.get_top_artists.await_count == 3  # short, medium, long term

    # Reset mock call counts
    trait_extractor.spotify_service.get_top_tracks.reset_mock()
    trait_extractor.spotify_service.get_top_artists.reset_mock()

    # Second call should hit cache
    traits2 = await trait_extractor.extract_music_traits(user_id)
    assert trait_extractor.spotify_service.get_top_tracks.await_count == 0
    assert trait_extractor.spotify_service.get_top_artists.await_count == 0
    assert traits2 == cached_music_traits

@pytest.mark.asyncio
async def test_trait_update_scheduling(trait_extractor, db_session):
    """Test that trait updates are scheduled correctly."""
    user_id = "test-user-1"

    # Update traits with UTC time
    now = datetime.now(pytz.UTC)
    await trait_extractor.update_user_traits(user_id)

    # Get the initial update timestamp using SQLAlchemy 2.0 style
    stmt = select(Traits).where(Traits.user_id == user_id)
    result = await db_session.execute(stmt)
    traits = result.scalar_one()
    initial_update = traits.last_updated_at.astimezone(pytz.UTC)

    # Wait a moment
    await asyncio.sleep(1)

    # Update again
    await trait_extractor.update_user_traits(user_id)

    # Get the new timestamp
    stmt = select(Traits).where(Traits.user_id == user_id)
    result = await db_session.execute(stmt)
    traits = result.scalar_one()
    second_update = traits.last_updated_at.astimezone(pytz.UTC)

    # Verify the timestamp was updated
    assert second_update > initial_update
    assert traits.next_update_at.astimezone(pytz.UTC) > second_update 