import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from app.models.event import EventModel
from app.services.cache_service import CacheService, CacheEntry, CacheConfig
from app.models.preferences import UserPreferences
import time

@pytest.fixture
async def cache_service():
    """Provide a clean cache service for each test."""
    service = CacheService()
    await service.clear_all()
    yield service
    await service.clear_all()

@pytest.fixture
def test_preferences():
    """Create test user preferences."""
    return UserPreferences(
        user_id="test-user",
        preferred_categories=["music", "sports"],
        excluded_categories=["business"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={
            "city": "New York",
            "coordinates": {"lat": 40.7128, "lng": -74.0060}
        }
    )

@pytest.fixture
def test_events():
    """Create test events."""
    return [
        EventModel(
            id=i,
            platform_id=f"event-{i}",
            title=f"Test Event {i}",
            description=f"Description {i}",
            start_datetime=datetime.utcnow() + timedelta(days=i),
            categories=["music"] if i % 2 == 0 else ["sports"],
            platform="test",
            url=f"http://test.com/event{i}",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            last_scraped_at=datetime.utcnow()
        )
        for i in range(5)
    ]

class TestCacheService:
    async def test_cache_recommendations(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test basic caching of recommendations."""
        # Cache recommendations
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events
        )
        
        # Retrieve from cache
        cached = await cache_service.get_recommendations(
            "test-user",
            test_preferences
        )
        
        assert cached is not None
        assert len(cached) == len(test_events)
        assert cached[0].id == test_events[0].id

    async def test_cache_expiration(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test cache expiration behavior."""
        # Override TTL for testing
        cache_service.configs['recommendations'] = CacheConfig(
            ttl_seconds=1,
            refresh_threshold_seconds=1,
            max_stale_seconds=2
        )
        
        # Cache recommendations
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events
        )
        
        # Verify cache hit
        cached = await cache_service.get_recommendations(
            "test-user",
            test_preferences
        )
        assert cached is not None
        
        # Wait for expiration
        time.sleep(2)
        
        # Verify cache miss
        expired = await cache_service.get_recommendations(
            "test-user",
            test_preferences
        )
        assert expired is None

    async def test_cache_invalidation(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test cache invalidation."""
        # Cache recommendations
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events
        )
        
        # Verify cache hit
        assert await cache_service.get_recommendations(
            "test-user",
            test_preferences
        ) is not None
        
        # Invalidate cache
        await cache_service.invalidate_recommendations(
            "test-user",
            test_preferences
        )
        
        # Verify cache miss
        assert await cache_service.get_recommendations(
            "test-user",
            test_preferences
        ) is None

    async def test_cache_context(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test caching with different contexts."""
        context1 = {"location": "New York", "time": "evening"}
        context2 = {"location": "New York", "time": "morning"}
        
        # Cache with different contexts
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events[:2],
            context1
        )
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events[2:],
            context2
        )
        
        # Verify different cache entries
        cached1 = await cache_service.get_recommendations(
            "test-user",
            test_preferences,
            context1
        )
        cached2 = await cache_service.get_recommendations(
            "test-user",
            test_preferences,
            context2
        )
        
        assert cached1 is not None
        assert cached2 is not None
        assert len(cached1) == 2
        assert len(cached2) == 3
        assert cached1[0].id != cached2[0].id

    async def test_access_stats(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test access statistics tracking."""
        # Cache recommendations
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events
        )
        
        # Access multiple times
        for _ in range(5):
            await cache_service.get_recommendations(
                "test-user",
                test_preferences
            )
        
        # Get cache stats
        stats = await cache_service.get_cache_stats()
        
        assert stats['local_cache']['size'] == 1
        assert len(stats['local_cache']['keys']) == 1
        assert stats['redis']['size'] >= 1

    async def test_high_traffic_behavior(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test cache behavior under high traffic."""
        # Override TTL for testing
        cache_service.configs['recommendations'] = CacheConfig(
            ttl_seconds=2,
            refresh_threshold_seconds=1,
            max_stale_seconds=4
        )
        
        # Cache recommendations
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events
        )
        
        # Simulate high traffic
        for _ in range(150):  # Above high traffic threshold
            await cache_service.get_recommendations(
                "test-user",
                test_preferences
            )
        
        # Wait for normal TTL
        time.sleep(2)
        
        # Should still serve stale data due to high traffic
        cached = await cache_service.get_recommendations(
            "test-user",
            test_preferences
        )
        assert cached is not None
        
        # Wait for max stale
        time.sleep(2)
        
        # Should now be expired
        expired = await cache_service.get_recommendations(
            "test-user",
            test_preferences
        )
        assert expired is None

    async def test_multi_layer_fallback(
        self,
        cache_service,
        test_preferences,
        test_events
    ):
        """Test fallback between cache layers."""
        # Cache recommendations
        await cache_service.set_recommendations(
            "test-user",
            test_preferences,
            test_events
        )
        
        # Clear local cache only
        cache_service.local_cache.clear()
        
        # Should still get data from Redis
        cached = await cache_service.get_recommendations(
            "test-user",
            test_preferences
        )
        assert cached is not None
        
        # Verify local cache was repopulated
        assert len(cache_service.local_cache) == 1 