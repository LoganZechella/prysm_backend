import pytest
import time
from unittest.mock import Mock, patch
from app.services.error_handling import (
    with_retry,
    circuit_breaker,
    CircuitBreaker,
    CircuitBreakerRegistry
)

def test_retry_decorator_success():
    mock_func = Mock(return_value="success")
    decorated = with_retry()(mock_func)
    
    result = decorated()
    
    assert result == "success"
    assert mock_func.call_count == 1

def test_retry_decorator_with_temporary_failure():
    mock_func = Mock(side_effect=[ValueError("temp error"), "success"])
    decorated = with_retry(max_retries=2, initial_delay=0.1)(mock_func)
    
    result = decorated()
    
    assert result == "success"
    assert mock_func.call_count == 2

def test_retry_decorator_max_retries_exceeded():
    mock_func = Mock(side_effect=ValueError("persistent error"))
    decorated = with_retry(max_retries=2, initial_delay=0.1)(mock_func)
    
    with pytest.raises(ValueError, match="persistent error"):
        decorated()
    
    assert mock_func.call_count == 2

def test_circuit_breaker_initial_state():
    breaker = CircuitBreaker()
    assert breaker.state == "CLOSED"
    assert breaker.can_execute() is True

def test_circuit_breaker_opens_after_failures():
    breaker = CircuitBreaker(failure_threshold=2)
    
    breaker.record_failure()
    assert breaker.state == "CLOSED"
    
    breaker.record_failure()
    assert breaker.state == "OPEN"
    assert breaker.can_execute() is False

def test_circuit_breaker_reset_after_timeout():
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=0)
    
    breaker.record_failure()
    assert breaker.state == "OPEN"
    
    time.sleep(0.1)  # Ensure reset_timeout has passed
    assert breaker.can_execute() is True
    assert breaker.state == "HALF_OPEN"

def test_circuit_breaker_registry_singleton():
    registry1 = CircuitBreakerRegistry()
    registry2 = CircuitBreakerRegistry()
    assert registry1 is registry2

def test_circuit_breaker_decorator():
    success_count = 0
    failure_count = 0
    
    @circuit_breaker("test_breaker")
    def test_function():
        nonlocal success_count, failure_count
        if failure_count < 2:
            failure_count += 1
            raise ValueError("test error")
        success_count += 1
        return "success"
    
    # First two calls should fail
    for _ in range(2):
        with pytest.raises(ValueError):
            test_function()
    
    # Third call should be blocked by circuit breaker
    with pytest.raises(Exception, match="Circuit breaker test_breaker is OPEN"):
        test_function()
    
    assert success_count == 0
    assert failure_count == 2

def test_retry_with_circuit_breaker():
    mock_func = Mock(side_effect=[ValueError("error1"), ValueError("error2"), "success"])
    decorated = with_retry(
        max_retries=3,
        initial_delay=0.1,
        circuit_breaker_name="test_retry_breaker"
    )(mock_func)
    
    result = decorated()
    
    assert result == "success"
    assert mock_func.call_count == 3

@patch('time.sleep')  # Mock sleep to speed up tests
def test_exponential_backoff(mock_sleep):
    mock_func = Mock(side_effect=[ValueError("error1"), ValueError("error2"), "success"])
    decorated = with_retry(
        max_retries=3,
        initial_delay=1.0,
        exponential_base=2.0
    )(mock_func)
    
    result = decorated()
    
    assert result == "success"
    assert mock_func.call_count == 3
    mock_sleep.assert_any_call(1.0)  # First retry
    mock_sleep.assert_any_call(2.0)  # Second retry with exponential backoff 