"""Redis cache configuration and utilities."""
from datetime import timedelta
import json
from typing import Any, Optional
from redis import Redis
from functools import wraps

from app.core.config import settings

# Initialize Redis client
redis_client = Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=True,
    socket_timeout=5,
    retry_on_timeout=True
)

# Constants
TRAIT_CACHE_TTL = timedelta(days=30)
API_RESPONSE_CACHE_TTL = timedelta(days=30)

class CacheManager:
    """Manager for Redis caching operations with monitoring."""
    
    @staticmethod
    def get_trait_cache_key(user_id: str, trait_type: str) -> str:
        """Generate cache key for traits."""
        return f"user:{user_id}:traits:{trait_type}"
    
    @staticmethod
    def get_api_cache_key(endpoint: str, params: str) -> str:
        """Generate cache key for API responses."""
        return f"api:{endpoint}:{params}"

    @classmethod
    def get_traits(cls, user_id: str, trait_type: str) -> Optional[dict]:
        """Get traits from cache."""
        key = cls.get_trait_cache_key(user_id, trait_type)
        try:
            data = redis_client.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            print(f"Cache get error for {key}: {str(e)}")
        return None

    @classmethod
    def set_traits(cls, user_id: str, trait_type: str, traits: dict) -> bool:
        """Set traits in cache."""
        key = cls.get_trait_cache_key(user_id, trait_type)
        try:
            redis_client.setex(
                key,
                TRAIT_CACHE_TTL,
                json.dumps(traits)
            )
            return True
        except Exception as e:
            print(f"Cache set error for {key}: {str(e)}")
            return False

    @classmethod
    def cache_api_response(cls, ttl: int = API_RESPONSE_CACHE_TTL.days):
        """Decorator for caching API responses."""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Generate cache key from function name and arguments
                cache_key = cls.get_api_cache_key(
                    func.__name__,
                    json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
                )
                
                # Try to get from cache
                try:
                    cached_data = redis_client.get(cache_key)
                    if cached_data:
                        return json.loads(cached_data)
                except Exception as e:
                    print(f"Cache get error in decorator for {cache_key}: {str(e)}")
                
                # If not in cache, call function and cache result
                result = await func(*args, **kwargs)
                try:
                    redis_client.setex(
                        cache_key,
                        timedelta(days=ttl),
                        json.dumps(result)
                    )
                except Exception as e:
                    print(f"Cache set error in decorator for {cache_key}: {str(e)}")
                
                return result
            return wrapper
        return decorator

# Cache monitoring
def get_cache_stats() -> dict[str, Any]:
    """Get cache statistics."""
    try:
        info = redis_client.info()
        return {
            "hits": info.get("keyspace_hits", 0),
            "misses": info.get("keyspace_misses", 0),
            "memory_used": info.get("used_memory_human", "0B"),
            "connected_clients": info.get("connected_clients", 0),
            "uptime_days": info.get("uptime_in_days", 0)
        }
    except Exception as e:
        print(f"Error getting cache stats: {str(e)}")
        return {} 