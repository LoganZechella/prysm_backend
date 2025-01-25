"""
Retry handler implementation for handling transient failures.

This module provides a RetryHandler class that implements retry logic:
- Configurable retry attempts
- Exponential backoff with jitter
- Custom retry conditions
- Retry statistics and monitoring
"""

import logging
import random
import asyncio
from typing import TypeVar, Callable, Awaitable, Optional, Any, Literal
from datetime import datetime

logger = logging.getLogger(__name__)

T = TypeVar('T')

class RetryError(Exception):
    """Exception raised when all retry attempts fail"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error

class RetryHandler:
    """Handler for retrying operations with exponential backoff"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: bool = True
    ):
        """
        Initialize the retry handler.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            jitter: Whether to add random jitter to delays
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = 'true' if jitter else 'false'
        
        # Statistics
        self.total_retries = 0
        self.successful_retries = 0
        self.failed_retries = 0
        self.last_retry = None
        
    def reset(self) -> None:
        """Reset retry statistics"""
        self.total_retries = 0
        self.successful_retries = 0
        self.failed_retries = 0
        self.last_retry = None
        logger.debug("Retry handler statistics reset")
        
    def _calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for current retry attempt.
        
        Args:
            attempt: Current retry attempt number
            
        Returns:
            Delay in seconds
        """
        # Calculate exponential backoff
        delay = min(
            self.base_delay * (2 ** attempt),
            self.max_delay
        )
        
        if self.jitter == 'true':
            # Add random jitter between -25% and +25%
            jitter = random.uniform(-0.25, 0.25)
            delay = delay * (1 + jitter)
            
        logger.debug(f"Calculated retry delay: {delay:.2f}s for attempt {attempt}")
        return delay
        
    async def execute_with_retry(
        self,
        operation: Callable[[], Awaitable[T]],
        should_retry: Optional[Callable[[Exception], Literal['true', 'false']]] = None
    ) -> T:
        """
        Execute an operation with retries.
        
        Args:
            operation: Async operation to execute
            should_retry: Optional function to determine if an error should trigger retry
            
        Returns:
            Result of successful operation
            
        Raises:
            RetryError: If all retry attempts fail
        """
        last_error = None
        start_time = datetime.utcnow()
        
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    delay = self._calculate_delay(attempt - 1)
                    logger.info(
                        f"Retrying operation after {delay:.2f}s "
                        f"(attempt {attempt}/{self.max_retries}, "
                        f"elapsed: {(datetime.utcnow() - start_time).total_seconds():.1f}s)"
                    )
                    await asyncio.sleep(delay)
                    
                # Execute the operation and await its result
                result = await operation()
                
                if attempt > 0:
                    self.successful_retries += 1
                    self.last_retry = datetime.utcnow()
                    logger.info(
                        f"Operation succeeded after {attempt} "
                        f"retries in {(datetime.utcnow() - start_time).total_seconds():.1f}s"
                    )
                    
                return result
                
            except Exception as e:
                last_error = e
                
                if attempt < self.max_retries:
                    # Check if we should retry this error
                    retry_result = should_retry(e) if callable(should_retry) else 'true'
                    if retry_result == 'false':
                        logger.info(
                            f"Not retrying operation due to error type: {type(e).__name__}\n"
                            f"Error: {str(e)}"
                        )
                        break
                        
                    self.total_retries += 1
                    logger.warning(
                        f"Operation failed (attempt {attempt + 1}/{self.max_retries})\n"
                        f"Error: {str(e)}\n"
                        f"Elapsed time: {(datetime.utcnow() - start_time).total_seconds():.1f}s"
                    )
                else:
                    self.failed_retries += 1
                    logger.error(
                        f"Operation failed after {self.max_retries} retries\n"
                        f"Error: {str(e)}\n"
                        f"Total time: {(datetime.utcnow() - start_time).total_seconds():.1f}s"
                    )
                    
        raise RetryError(
            f"Operation failed after {attempt} attempts ({(datetime.utcnow() - start_time).total_seconds():.1f}s)",
            original_error=last_error
        )
        
    def get_stats(self) -> dict:
        """Get retry statistics"""
        return {
            'total_retries': self.total_retries,
            'successful_retries': self.successful_retries,
            'failed_retries': self.failed_retries,
            'success_rate': (
                self.successful_retries / self.total_retries 
                if self.total_retries > 0 else 0
            ),
            'last_retry': self.last_retry.isoformat() if self.last_retry else None,
            'jitter_enabled': self.jitter
        } 