"""
Circuit breaker implementation for handling scraper failures.

This module provides a CircuitBreaker class that implements the circuit breaker pattern:
- Tracks failure rates
- Automatically opens circuit when failure threshold is exceeded
- Implements half-open state for recovery
- Provides monitoring and metrics
"""

import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, Callable, Awaitable, TypeVar, Generic, Literal

logger = logging.getLogger(__name__)

T = TypeVar('T')

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"     # Service disabled
    HALF_OPEN = "half_open"  # Testing recovery

class CircuitBreaker(Generic[T]):
    """Circuit breaker implementation for protecting against cascading failures."""
    
    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
        half_open_max_tries: int = 1
    ):
        """Initialize the circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
            half_open_max_tries: Number of successful tries needed to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_tries = half_open_max_tries
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_successes = 0
        
    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_successes = 0
        logger.info("Circuit breaker reset to initial state")
        
    def _is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN
        
    def _should_attempt_recovery(self) -> Literal['true', 'false']:
        """Check if we should attempt recovery."""
        if not self.last_failure_time:
            return 'false'
            
        elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
        return 'true' if elapsed >= self.recovery_timeout else 'false'
        
    def _enter_half_open(self) -> None:
        """Enter half-open state."""
        self.state = CircuitState.HALF_OPEN
        self.half_open_successes = 0
        logger.info("Circuit breaker entering half-open state")
        
    def _close_circuit(self) -> None:
        """Close the circuit."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_successes = 0
        logger.info("Circuit breaker closed")
        
    def _on_success(self) -> None:
        """Handle successful operation."""
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_successes += 1
            if self.half_open_successes >= self.half_open_max_tries:
                self._close_circuit()
                
    def _on_failure(self) -> None:
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.state == CircuitState.HALF_OPEN or self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.info("Circuit breaker opened")
            
    async def execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """Execute an operation with circuit breaker protection.

        Args:
            operation: Async operation to execute.

        Returns:
            Result of successful operation.

        Raises:
            CircuitBreakerError: If circuit is open.
        """
        if self._is_open():
            if self._should_attempt_recovery() == 'true':
                self._enter_half_open()
            else:
                raise CircuitBreakerError("Circuit breaker is open")

        try:
            # Execute the operation and await its result
            result = await operation()
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise
        
    async def _handle_failure(
        self,
        error: Exception,
        fallback: Optional[Callable[[], Awaitable[T]]]
    ) -> T:
        """Handle operation failure"""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        logger.debug(f"Failure count: {self.failure_count}/{self.failure_threshold}")
        
        # Check if we should open the circuit
        if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
            self._open_circuit()
            
        # If in half-open state, any failure opens the circuit
        elif self.state == CircuitState.HALF_OPEN:
            self._open_circuit()
            
        if fallback:
            try:
                logger.debug("Attempting fallback operation")
                return await fallback()
            except Exception as fallback_error:
                logger.error(f"Fallback operation failed: {str(fallback_error)}")
                raise CircuitBreakerError(
                    f"Circuit breaker is {self.state.value} and fallback failed",
                    original_error=error
                )
            
        raise CircuitBreakerError(
            f"Circuit breaker is {self.state.value}",
            original_error=error
        )
        
    async def _handle_open_circuit(
        self,
        fallback: Optional[Callable[[], Awaitable[T]]]
    ) -> T:
        """Handle requests when circuit is open"""
        if fallback:
            try:
                logger.debug("Circuit is open, executing fallback")
                return await fallback()
            except Exception as fallback_error:
                logger.error(f"Fallback operation failed: {str(fallback_error)}")
                raise CircuitBreakerError(
                    f"Circuit breaker is {self.state.value} and fallback failed",
                    original_error=fallback_error
                )
            
        raise CircuitBreakerError(f"Circuit breaker is {self.state.value}")
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get current circuit breaker metrics"""
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'last_state_change': self.last_state_change.isoformat(),
            'half_open_successes': self.half_open_successes,
            'is_closed': 'true' if self.state == CircuitState.CLOSED else 'false',
            'is_open': 'true' if self.state == CircuitState.OPEN else 'false',
            'is_half_open': 'true' if self.state == CircuitState.HALF_OPEN else 'false'
        }

class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker prevents operation"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error 