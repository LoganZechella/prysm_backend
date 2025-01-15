from functools import wraps
import time
from typing import Any, Callable, TypeVar, Dict, Optional
from datetime import datetime, timedelta
import logging
from app.monitoring.performance import PerformanceMonitor

T = TypeVar('T')

logger = logging.getLogger(__name__)

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_time = datetime.now()
        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            
    def record_success(self) -> None:
        self.failures = 0
        self.state = "CLOSED"
        self.last_failure_time = None
        
    def can_execute(self) -> bool:
        if self.state == "CLOSED":
            return True
            
        if self.state == "OPEN":
            if self.last_failure_time and \
               datetime.now() - self.last_failure_time > timedelta(seconds=self.reset_timeout):
                self.state = "HALF_OPEN"
                return True
            return False
            
        # HALF_OPEN state
        return True

class CircuitBreakerRegistry:
    _instance = None
    _breakers: Dict[str, CircuitBreaker] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_breaker(self, name: str) -> CircuitBreaker:
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker()
        return self._breakers[name]

def with_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    exponential_base: float = 2.0,
    exceptions: tuple = (Exception,),
    circuit_breaker_name: Optional[str] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that implements retry logic with exponential backoff and circuit breaker pattern.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        exceptions: Tuple of exceptions to catch and retry
        circuit_breaker_name: Name of the circuit breaker to use
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = initial_delay
            last_exception = None
            performance_monitor = PerformanceMonitor()
            
            # Get circuit breaker if name provided
            circuit_breaker = None
            if circuit_breaker_name:
                circuit_breaker = CircuitBreakerRegistry().get_breaker(circuit_breaker_name)
            
            for attempt in range(max_retries):
                try:
                    # Check circuit breaker state
                    if circuit_breaker and not circuit_breaker.can_execute():
                        raise Exception(f"Circuit breaker {circuit_breaker_name} is OPEN")
                    
                    with performance_monitor.monitor_api_call(func.__name__):
                        result = func(*args, **kwargs)
                    
                    # Record success if using circuit breaker
                    if circuit_breaker:
                        circuit_breaker.record_success()
                    
                    return result
                    
                except exceptions as e:
                    last_exception = e
                    
                    # Record failure if using circuit breaker
                    if circuit_breaker:
                        circuit_breaker.record_failure()
                    
                    if attempt == max_retries - 1:
                        logger.error(f"Final retry attempt failed for {func.__name__}: {str(e)}")
                        raise
                    
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {delay} seconds..."
                    )
                    
                    time.sleep(delay)
                    delay *= exponential_base
            
            raise last_exception if last_exception else Exception("Unexpected error")
            
        return wrapper
    return decorator

def circuit_breaker(name: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that implements circuit breaker pattern without retries.
    
    Args:
        name: Name of the circuit breaker to use
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            breaker = CircuitBreakerRegistry().get_breaker(name)
            
            if not breaker.can_execute():
                raise Exception(f"Circuit breaker {name} is OPEN")
            
            try:
                result = func(*args, **kwargs)
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
                
        return wrapper
    return decorator 