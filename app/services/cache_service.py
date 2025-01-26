from typing import List, Optional, Dict, Any, TypeVar, Generic
from datetime import datetime, timedelta
import json
from redis import Redis
from pydantic import BaseModel
import logging
from app.models.event import EventModel
from app.models.preferences import UserPreferences

logger = logging.getLogger(__name__)

T = TypeVar('T')

class CacheConfig(BaseModel):
    """Configuration for cache behavior"""
    ttl_seconds: int
    refresh_threshold_seconds: int
    max_stale_seconds: int

class CacheEntry(BaseModel, Generic[T]):
    """Generic cache entry with metadata"""
    data: T
    created_at: datetime
    updated_at: datetime
    access_count: int
    version: int
    source: str

class CacheService:
    """Multi-layer caching service for recommendations"""
    
    def __init__(self):
        """Initialize cache layers"""
        self.redis = Redis(host="localhost", port=6379, db=0)
        self.local_cache: Dict[str, CacheEntry] = {}
        
        # Default cache configurations
        self.configs = {
            'recommendations': CacheConfig(
                ttl_seconds=3600,  # 1 hour
                refresh_threshold_seconds=1800,  # 30 minutes
                max_stale_seconds=7200  # 2 hours
            ),
            'events': CacheConfig(
                ttl_seconds=7200,  # 2 hours
                refresh_threshold_seconds=3600,  # 1 hour
                max_stale_seconds=14400  # 4 hours
            ),
            'preferences': CacheConfig(
                ttl_seconds=1800,  # 30 minutes
                refresh_threshold_seconds=900,  # 15 minutes
                max_stale_seconds=3600  # 1 hour
            )
        }

    async def get_recommendations(
        self,
        user_id: str,
        preferences: UserPreferences,
        context: Dict[str, Any] = None
    ) -> Optional[List[EventModel]]:
        """
        Get cached recommendations with smart refresh strategy.
        
        Args:
            user_id: User identifier
            preferences: User preferences
            context: Additional context for cache key generation
            
        Returns:
            Cached recommendations if available
        """
        cache_key = self._generate_cache_key('recommendations', user_id, preferences, context)
        
        try:
            # Try local cache first
            if cache_key in self.local_cache:
                entry = self.local_cache[cache_key]
                if not self._is_expired(entry, 'recommendations'):
                    self._update_access_stats(cache_key, entry)
                    return entry.data
            
            # Try Redis cache
            redis_data = self.redis.get(cache_key)
            if redis_data:
                entry = CacheEntry.parse_raw(redis_data)
                if not self._is_expired(entry, 'recommendations'):
                    # Update local cache
                    self.local_cache[cache_key] = entry
                    self._update_access_stats(cache_key, entry)
                    return entry.data
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving recommendations from cache: {str(e)}")
            return None

    async def set_recommendations(
        self,
        user_id: str,
        preferences: UserPreferences,
        recommendations: List[EventModel],
        context: Dict[str, Any] = None
    ) -> None:
        """
        Cache recommendations with metadata.
        
        Args:
            user_id: User identifier
            preferences: User preferences
            recommendations: Recommendations to cache
            context: Additional context for cache key generation
        """
        cache_key = self._generate_cache_key('recommendations', user_id, preferences, context)
        
        try:
            # Create cache entry
            entry = CacheEntry(
                data=recommendations,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                access_count=0,
                version=1,
                source='recommendation_engine'
            )
            
            # Update local cache
            self.local_cache[cache_key] = entry
            
            # Update Redis cache
            self.redis.setex(
                cache_key,
                self.configs['recommendations'].ttl_seconds,
                entry.json()
            )
            
        except Exception as e:
            logger.error(f"Error caching recommendations: {str(e)}")

    async def invalidate_recommendations(
        self,
        user_id: str,
        preferences: Optional[UserPreferences] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Invalidate cached recommendations.
        
        Args:
            user_id: User identifier
            preferences: Optional preferences for specific invalidation
            context: Additional context for cache key generation
        """
        try:
            # Generate cache key pattern
            if preferences and context:
                # Invalidate specific cache entry
                cache_key = self._generate_cache_key('recommendations', user_id, preferences, context)
                self.local_cache.pop(cache_key, None)
                self.redis.delete(cache_key)
            else:
                # Invalidate all user's recommendations
                pattern = f"recommendations:{user_id}:*"
                # Clear local cache
                keys_to_remove = [k for k in self.local_cache.keys() if k.startswith(pattern)]
                for key in keys_to_remove:
                    self.local_cache.pop(key)
                # Clear Redis cache
                for key in self.redis.scan_iter(pattern):
                    self.redis.delete(key)
                    
        except Exception as e:
            logger.error(f"Error invalidating recommendations cache: {str(e)}")

    def _generate_cache_key(
        self,
        prefix: str,
        user_id: str,
        preferences: UserPreferences,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate deterministic cache key."""
        # Create base key
        key_parts = [prefix, user_id]
        
        # Add preference hash
        pref_hash = hash(frozenset(preferences.dict().items()))
        key_parts.append(str(pref_hash))
        
        # Add context hash if provided
        if context:
            context_hash = hash(frozenset(context.items()))
            key_parts.append(str(context_hash))
        
        return ':'.join(key_parts)

    def _is_expired(self, entry: CacheEntry, config_key: str) -> bool:
        """Check if cache entry is expired based on configuration."""
        config = self.configs[config_key]
        now = datetime.utcnow()
        age = (now - entry.updated_at).total_seconds()
        
        # Hard TTL check
        if age > config.max_stale_seconds:
            return True
            
        # Soft TTL check (for refresh)
        if age > config.ttl_seconds:
            # Allow stale data if frequently accessed
            if entry.access_count > 100:  # High traffic threshold
                return age > config.max_stale_seconds
            return True
            
        # Refresh threshold check
        if age > config.refresh_threshold_seconds:
            # Probabilistic refresh based on access pattern
            if entry.access_count < 10:  # Low traffic
                return True
            # High traffic, but old data
            if age > (config.ttl_seconds * 0.8):  # 80% of TTL
                return True
                
        return False

    def _update_access_stats(self, cache_key: str, entry: CacheEntry) -> None:
        """Update cache entry access statistics."""
        try:
            entry.access_count += 1
            # Update Redis if access count crosses thresholds
            if entry.access_count in [10, 50, 100, 500, 1000]:
                self.redis.setex(
                    cache_key,
                    self.configs['recommendations'].ttl_seconds,
                    entry.json()
                )
        except Exception as e:
            logger.error(f"Error updating cache stats: {str(e)}")

    async def clear_all(self) -> None:
        """Clear all caches (for testing/maintenance)."""
        try:
            self.local_cache.clear()
            self.redis.flushdb()
        except Exception as e:
            logger.error(f"Error clearing caches: {str(e)}")

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        try:
            stats = {
                'local_cache': {
                    'size': len(self.local_cache),
                    'keys': list(self.local_cache.keys())
                },
                'redis': {
                    'size': self.redis.dbsize(),
                    'memory_used': self.redis.info()['used_memory_human']
                }
            }
            return stats
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {} 