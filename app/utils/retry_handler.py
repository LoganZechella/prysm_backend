"""
Retry handler implementation for managing request retries.

This module provides a RetryHandler class that implements exponential backoff
for retrying failed requests. It supports:
- Maximum retry attempts
- Exponential backoff with jitter
- Configurable retry conditions
- Detailed retry statistics
"""

import asyncio
import random
import logging
from typing import Callable, Optional, Any, Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

class RetryError(Exception):
    """Exception raised when all retry attempts have failed"""
    
    def __init__(self, message: str, last_error: Optional[Exception] = None):
        """Initialize retry error"""
        super().__init__(message)
        self.last_error = last_error

class RetryHandler:
    """Handles request retries with exponential backoff"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True
    ):
        """
        Initialize the retry handler.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            jitter: Whether to add random jitter to delays
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        
        # Stats for monitoring
        self.stats = {
            'total_attempts': 0,
            'successful_retries': 0,
            'failed_retries': 0,
            'total_retry_delay': 0.0
        }
        
        # Track recent failures for monitoring
        self.recent_failures: List[Dict[str, Any]] = []
        
    def _calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for current retry attempt.
        
        Args:
            attempt: Current retry attempt number (1-based)
            
        Returns:
            Delay in seconds
        """
        delay = min(
            self.max_delay,
            self.base_delay * (2 ** (attempt - 1))
        )
        
        if self.jitter:
            # Add random jitter between -25% and +25%
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
            
        return max(0, delay)
        
    async def execute_with_retry(
        self,
        operation: Callable,
        should_retry: Optional[Callable[[Exception], bool]] = None,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute an operation with retries.
        
        Args:
            operation: Async function to execute
            should_retry: Optional function to determine if an error should trigger retry
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result from successful operation execution
            
        Raises:
            RetryError: When all retry attempts fail
        """
        last_exception = None
        attempt = 0
        
        while attempt <= self.max_retries:
            try:
                self.stats['total_attempts'] += 1
                result = await operation(*args, **kwargs)
                
                if attempt > 0:
                    self.stats['successful_retries'] += 1
                    
                return result
                
            except Exception as e:
                last_exception = e
                attempt += 1
                
                # Check if we should retry this error
                if should_retry and not should_retry(e):
                    logger.info(
                        f"Not retrying operation after attempt {attempt} due to error type: {type(e).__name__}"
                    )
                    break
                    
                # Check if we've exceeded max retries
                if attempt > self.max_retries:
                    self.stats['failed_retries'] += 1
                    self._record_failure(operation.__name__, str(e))
                    logger.error(
                        f"Operation failed after {attempt} attempts. Last error: {str(e)}"
                    )
                    break
                    
                # Calculate and apply backoff delay
                delay = self._calculate_delay(attempt)
                self.stats['total_retry_delay'] += delay
                
                logger.warning(
                    f"Retry attempt {attempt}/{self.max_retries} after error: {str(e)}. "
                    f"Waiting {delay:.2f}s before next attempt."
                )
                
                await asyncio.sleep(delay)
                
        if last_exception:
            raise RetryError(
                f"Operation failed after {attempt} attempts",
                last_error=last_exception
            )
            
    def _record_failure(self, operation: str, error: str) -> None:
        """Record a failed operation for monitoring"""
        self.recent_failures.append({
            'timestamp': datetime.utcnow(),
            'operation': operation,
            'error': error
        })
        
        # Keep only last 100 failures
        if len(self.recent_failures) > 100:
            self.recent_failures.pop(0)
            
    def get_stats(self) -> Dict[str, Any]:
        """Get current retry statistics"""
        return {
            'total_attempts': self.stats['total_attempts'],
            'successful_retries': self.stats['successful_retries'],
            'failed_retries': self.stats['failed_retries'],
            'success_rate': (
                (self.stats['successful_retries'] / self.stats['total_attempts'] * 100)
                if self.stats['total_attempts'] > 0
                else 100.0
            ),
            'average_retry_delay': (
                self.stats['total_retry_delay'] / self.stats['successful_retries']
                if self.stats['successful_retries'] > 0
                else 0.0
            ),
            'recent_failures': self.recent_failures[-5:]  # Last 5 failures
        } 