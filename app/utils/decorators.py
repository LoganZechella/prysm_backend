"""Utility decorators for error handling and resilience."""

import time
import logging
import functools
from typing import Type, Tuple, Optional, Callable, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: int = 60,
        half_open_timeout: int = 30
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            reset_timeout: Seconds to wait before resetting (closing) circuit
            half_open_timeout: Seconds to wait in half-open state
        """
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_timeout = half_open_timeout
        
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"
        
    def record_failure(self):
        """Record a failure and potentially open the circuit."""
        self.failures += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.failures >= self.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.failures} failures")
            
    def record_success(self):
        """Record a success and potentially close the circuit."""
        self.failures = 0
        self.last_failure_time = None
        self.state = "closed"
        
    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == "closed":
            return True
            
        if self.state == "open":
            if self.last_failure_time:
                if datetime.utcnow() - self.last_failure_time > timedelta(seconds=self.reset_timeout):
                    self.state = "half-open"
                    return True
            return False
            
        # Half-open state
        if datetime.utcnow() - self.last_failure_time > timedelta(seconds=self.half_open_timeout):
            return True
        return False

# Global circuit breakers
_circuit_breakers = {}

def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    reset_timeout: int = 60,
    half_open_timeout: int = 30
) -> Callable:
    """
    Circuit breaker decorator.
    
    Args:
        name: Circuit breaker name
        failure_threshold: Number of failures before opening circuit
        reset_timeout: Seconds to wait before resetting circuit
        half_open_timeout: Seconds to wait in half-open state
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if name not in _circuit_breakers:
                _circuit_breakers[name] = CircuitBreaker(
                    failure_threshold=failure_threshold,
                    reset_timeout=reset_timeout,
                    half_open_timeout=half_open_timeout
                )
                
            breaker = _circuit_breakers[name]
            
            if not breaker.can_execute():
                raise Exception(f"Circuit breaker {name} is open")
                
            try:
                result = func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
                
        return wrapper
    return decorator

def with_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    circuit_breaker_name: Optional[str] = None
) -> Callable:
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_retries: Maximum number of retries
        initial_delay: Initial delay between retries in seconds
        exponential_base: Base for exponential backoff
        exceptions: Tuple of exceptions to catch
        circuit_breaker_name: Optional circuit breaker to use
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        if circuit_breaker_name and circuit_breaker_name in _circuit_breakers:
                            _circuit_breakers[circuit_breaker_name].record_failure()
                        raise last_exception
                        
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed: {str(e)}. "
                        f"Retrying in {delay:.1f} seconds..."
                    )
                    
                    time.sleep(delay)
                    delay *= exponential_base
                    
            raise last_exception
            
        return wrapper
    return decorator 