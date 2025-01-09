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
from typing import Dict, Any, Optional, Callable, Awaitable
import asyncio

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"     # Service disabled
    HALF_OPEN = "half_open"  # Testing recovery

class CircuitBreaker:
    """Circuit breaker implementation for handling service failures"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_tries: int = 3,
        reset_timeout: float = 300.0
    ):
        """
        Initialize the circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            half_open_max_tries: Max attempts in half-open state
            reset_timeout: Seconds after which to reset failure count
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_tries = half_open_max_tries
        self.reset_timeout = reset_timeout
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.last_state_change = datetime.utcnow()
        self.half_open_successes = 0
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
    async def execute(
        self,
        operation: Callable[..., Awaitable[Any]],
        fallback: Optional[Callable[..., Awaitable[Any]]] = None,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute an operation with circuit breaker protection.
        
        Args:
            operation: Async function to execute
            fallback: Optional fallback function if circuit is open
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result from operation or fallback
            
        Raises:
            CircuitBreakerError if circuit is open and no fallback provided
        """
        async with self._lock:
            # Check if we should reset failure count
            if (
                self.last_failure_time and
                (datetime.utcnow() - self.last_failure_time).total_seconds() > self.reset_timeout
            ):
                self._reset()
                
            # Handle different circuit states
            if self.state == CircuitState.OPEN:
                if await self._should_attempt_recovery():
                    self._transition_to_half_open()
                else:
                    return await self._handle_open_circuit(fallback, *args, **kwargs)
                    
            try:
                result = await operation(*args, **kwargs)
                
                # Handle success
                if self.state == CircuitState.HALF_OPEN:
                    self.half_open_successes += 1
                    if self.half_open_successes >= self.half_open_max_tries:
                        self._close_circuit()
                        
                return result
                
            except Exception as e:
                return await self._handle_failure(e, fallback, *args, **kwargs)
                
    async def _handle_failure(
        self,
        error: Exception,
        fallback: Optional[Callable[..., Awaitable[Any]]],
        *args,
        **kwargs
    ) -> Any:
        """Handle operation failure"""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        # Check if we should open the circuit
        if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
            self._open_circuit()
            
        # If in half-open state, any failure opens the circuit
        elif self.state == CircuitState.HALF_OPEN:
            self._open_circuit()
            
        if fallback:
            return await fallback(*args, **kwargs)
            
        raise CircuitBreakerError(
            f"Circuit breaker is {self.state.value}",
            original_error=error
        )
        
    async def _handle_open_circuit(
        self,
        fallback: Optional[Callable[..., Awaitable[Any]]],
        *args,
        **kwargs
    ) -> Any:
        """Handle requests when circuit is open"""
        if fallback:
            return await fallback(*args, **kwargs)
            
        raise CircuitBreakerError(f"Circuit breaker is {self.state.value}")
        
    async def _should_attempt_recovery(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if not self.last_state_change:
            return False
            
        time_since_open = (datetime.utcnow() - self.last_state_change).total_seconds()
        return time_since_open >= self.recovery_timeout
        
    def _transition_to_half_open(self) -> None:
        """Transition circuit to half-open state"""
        self.state = CircuitState.HALF_OPEN
        self.half_open_successes = 0
        self.last_state_change = datetime.utcnow()
        
        logger.info(
            "Circuit breaker transitioning to half-open state",
            extra={'state': self.state.value}
        )
        
    def _open_circuit(self) -> None:
        """Open the circuit"""
        self.state = CircuitState.OPEN
        self.last_state_change = datetime.utcnow()
        
        logger.warning(
            "Circuit breaker opened",
            extra={
                'state': self.state.value,
                'failure_count': self.failure_count
            }
        )
        
    def _close_circuit(self) -> None:
        """Close the circuit"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.last_state_change = datetime.utcnow()
        self.half_open_successes = 0
        
        logger.info(
            "Circuit breaker closed",
            extra={'state': self.state.value}
        )
        
    def _reset(self) -> None:
        """Reset circuit breaker state"""
        self.failure_count = 0
        self.last_failure_time = None
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get current circuit breaker metrics"""
        return {
            'state': self.state.value,
            'failure_count': self.failure_count,
            'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'last_state_change': self.last_state_change.isoformat(),
            'half_open_successes': self.half_open_successes
        }
        
class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker prevents operation"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error 