from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging
import json
from redis import Redis
from app.models.event import EventModel
from app.models.preferences import UserPreferencesBase
from app.utils.logging import setup_logger
from pydantic import BaseModel

logger = setup_logger(__name__)

class CacheEntry(BaseModel):
    """Model for cache entries."""
    data: Any
    created_at: datetime
    updated_at: datetime
    access_count: int
    version: int
    source: str

class CacheConfig(BaseModel):
    """Configuration for cache behavior."""
    ttl_seconds: int = 3600  # 1 hour
    refresh_threshold: float = 0.8  # Refresh when 80% of TTL has passed
    max_size: int = 1000  # Maximum number of entries in local cache

class CacheService:
    """Service for caching recommendations with smart refresh strategy."""
    
    def __init__(self):
        """Initialize cache service."""
        self.redis = Redis(host='localhost', port=6379, db=0)
        self.local_cache: Dict[str, CacheEntry] = {}
        self.configs = {
            'recommendations': CacheConfig(ttl_seconds=3600),  # 1 hour
            'trending': CacheConfig(ttl_seconds=1800),  # 30 minutes
            'similar': CacheConfig(ttl_seconds=7200)  # 2 hours
        }
    
    async def get_recommendations(
        self,
        user_id: str,
        preferences: UserPreferencesBase,
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
        preferences: UserPreferencesBase,
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
        preferences: Optional[UserPreferencesBase] = None,
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
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        try:
            return {
                'local_cache_size': len(self.local_cache),
                'redis_info': self.redis.info(),
                'configs': {k: v.dict() for k, v in self.configs.items()}
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return {}
    
    def _generate_cache_key(
        self,
        prefix: str,
        user_id: str,
        preferences: UserPreferencesBase,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate a unique cache key."""
        key_parts = [prefix, user_id]
        
        # Add preferences hash
        pref_hash = hash(json.dumps(preferences.model_dump(), sort_keys=True))
        key_parts.append(str(pref_hash))
        
        # Add context hash if provided
        if context:
            context_hash = hash(json.dumps(context, sort_keys=True))
            key_parts.append(str(context_hash))
        
        return ":".join(key_parts)
    
    def _is_expired(self, entry: CacheEntry, config_key: str) -> bool:
        """Check if a cache entry is expired."""
        config = self.configs[config_key]
        age = (datetime.utcnow() - entry.created_at).total_seconds()
        return age > config.ttl_seconds
    
    def _update_access_stats(self, key: str, entry: CacheEntry) -> None:
        """Update cache entry access statistics."""
        entry.access_count += 1
        entry.updated_at = datetime.utcnow()
        self.local_cache[key] = entry 