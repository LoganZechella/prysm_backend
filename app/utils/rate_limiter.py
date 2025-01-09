import asyncio
import time
from typing import Optional

class RateLimiter:
    """Rate limiter for Scrapfly API requests"""
    
    def __init__(self, rate_limit: int = 10):
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
        
    async def acquire(self):
        """Acquire a token, waiting if necessary"""
        async with self.lock:
            now = time.monotonic()
            time_passed = now - self.last_update
            self.tokens = min(self.rate_limit, 
                            self.tokens + time_passed * self.rate_limit)
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate_limit
                await asyncio.sleep(wait_time)
                self.tokens = 1
                
            self.tokens -= 1
            self.last_update = now 