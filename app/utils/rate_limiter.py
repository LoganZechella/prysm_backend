"""
Rate Limiter implementation for managing API request rates.

This module provides a RateLimiter class that implements token bucket algorithm
for rate limiting API requests. It supports:
- Per-second rate limiting
- Burst allowance
- Async/await interface
- Request queuing when limit is reached
"""

import asyncio
import time
import logging
from typing import Optional, Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

class RateLimiter:
    """Token bucket rate limiter implementation"""
    
    def __init__(
        self,
        requests_per_second: float,
        burst_limit: Optional[int] = None,
        max_delay: float = 60.0
    ):
        """
        Initialize the rate limiter.
        
        Args:
            requests_per_second: Maximum sustained request rate
            burst_limit: Maximum burst size (defaults to 2x requests_per_second)
            max_delay: Maximum time to wait for a token in seconds
        """
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit or int(requests_per_second * 2)
        self.max_delay = max_delay
        
        # Token bucket state
        self.tokens = self.burst_limit
        self.last_update = time.monotonic()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Stats for monitoring
        self.stats = {
            'total_requests': 0,
            'delayed_requests': 0,
            'dropped_requests': 0,
            'total_delay': 0.0,
            'last_request': None,
            'request_times': []  # List of last 100 request timestamps
        }
        
    def reset(self) -> None:
        """Reset rate limiter state"""
        self.tokens = self.burst_limit
        self.last_update = time.monotonic()
        self.stats = {
            'total_requests': 0,
            'delayed_requests': 0,
            'dropped_requests': 0,
            'total_delay': 0.0,
            'last_request': None,
            'request_times': []
        }
        logger.debug("Rate limiter reset to initial state")
        
    async def acquire(self) -> bool:
        """
        Acquire a rate limit token.
        
        Returns:
            True if token was acquired, False if max_delay was exceeded
        """
        async with self._lock:
            now = time.monotonic()
            time_passed = now - self.last_update
            
            # Add new tokens based on time passed
            new_tokens = time_passed * self.requests_per_second
            self.tokens = min(self.tokens + new_tokens, self.burst_limit)
            self.last_update = now
            
            # Update request times list (keep last 100)
            self.stats['request_times'].append(now)
            if len(self.stats['request_times']) > 100:
                self.stats['request_times'] = self.stats['request_times'][-100:]
            
            # If we have tokens available, consume one immediately
            if self.tokens >= 1:
                self.tokens -= 1
                self.stats['total_requests'] += 1
                self.stats['last_request'] = datetime.utcnow()
                logger.debug(f"Token acquired immediately (remaining: {self.tokens:.2f})")
                return True
                
            # Calculate required wait time for next token
            required_wait = (1 - self.tokens) / self.requests_per_second
            
            # Check if wait time exceeds max delay
            if required_wait > self.max_delay:
                self.stats['dropped_requests'] += 1
                logger.warning(
                    f"Rate limit exceeded, required wait {required_wait:.2f}s "
                    f"exceeds max delay {self.max_delay}s"
                )
                return False
                
            # Wait for next token
            self.stats['delayed_requests'] += 1
            self.stats['total_delay'] += required_wait
            logger.debug(f"Waiting {required_wait:.2f}s for next token")
            await asyncio.sleep(required_wait)
            
            # Consume token and update state
            self.tokens -= 1
            self.stats['total_requests'] += 1
            self.stats['last_request'] = datetime.utcnow()
            logger.debug(f"Token acquired after delay (remaining: {self.tokens:.2f})")
            return True
            
    async def __aenter__(self):
        """Async context manager interface"""
        success = await self.acquire()
        if not success:
            raise RuntimeError(
                f"Failed to acquire rate limit token (max delay {self.max_delay}s exceeded)"
            )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        pass
        
    def get_stats(self) -> Dict[str, float]:
        """Get current rate limiter statistics"""
        now = time.monotonic()
        recent_requests = len([
            t for t in self.stats['request_times']
            if now - t <= 60.0  # Requests in last minute
        ])
        
        return {
            'total_requests': self.stats['total_requests'],
            'delayed_requests': self.stats['delayed_requests'],
            'dropped_requests': self.stats['dropped_requests'],
            'average_delay': (
                self.stats['total_delay'] / self.stats['delayed_requests']
                if self.stats['delayed_requests'] > 0
                else 0.0
            ),
            'current_tokens': self.tokens,
            'requests_per_minute': recent_requests,
            'last_request': (
                self.stats['last_request'].isoformat()
                if self.stats['last_request']
                else None
            )
        } 