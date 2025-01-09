"""
Tests for monitoring and circuit breaker functionality.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock
import aiohttp

from app.utils.monitoring import ScraperMonitor
from app.utils.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerError

# Test data
TEST_SCRAPER = "test_scraper"
TEST_ERROR = Exception("Test error")

@pytest.fixture
def monitor():
    """Create a ScraperMonitor instance for testing"""
    return ScraperMonitor(error_threshold=0.5, window_minutes=5)

@pytest.fixture
def circuit_breaker():
    """Create a CircuitBreaker instance for testing"""
    return CircuitBreaker(
        failure_threshold=3,
        recovery_timeout=1.0,
        half_open_max_tries=2
    )

def test_monitor_record_error(monitor):
    """Test error recording in monitor"""
    context = {'url': 'https://test.com'}
    monitor.record_error(TEST_SCRAPER, TEST_ERROR, context)
    
    metrics = monitor.get_metrics(TEST_SCRAPER)
    assert metrics['error_count'] == 1
    
    # Check error details
    error = monitor.errors[TEST_SCRAPER][0]
    assert error['error_type'] == 'Exception'
    assert error['error_message'] == 'Test error'
    assert error['context'] == context
    
def test_monitor_record_request(monitor):
    """Test request recording in monitor"""
    # Record successful request
    monitor.record_request(TEST_SCRAPER, True, 0.5)
    metrics = monitor.get_metrics(TEST_SCRAPER)
    assert metrics['request_count'] == 1
    assert metrics['success_rate'] == 100.0
    
    # Record failed request
    monitor.record_request(TEST_SCRAPER, False, 0.3)
    metrics = monitor.get_metrics(TEST_SCRAPER)
    assert metrics['request_count'] == 2
    assert metrics['success_rate'] == 50.0
    assert abs(metrics['avg_response_time'] - 0.4) < 0.01
    
def test_monitor_error_threshold(monitor):
    """Test error threshold monitoring"""
    # Record requests to exceed threshold
    for _ in range(3):
        monitor.record_request(TEST_SCRAPER, False, 0.1)
        
    for _ in range(2):
        monitor.record_request(TEST_SCRAPER, True, 0.1)
        
    assert not monitor.get_health_status(TEST_SCRAPER)
    assert monitor.get_error_rate(TEST_SCRAPER) == 0.6
    
def test_monitor_window_trimming(monitor):
    """Test old data trimming"""
    # Add old data
    old_time = datetime.utcnow() - timedelta(minutes=10)
    monitor.errors[TEST_SCRAPER].append({
        'timestamp': old_time,
        'error_type': 'Exception',
        'error_message': 'Old error',
        'context': {}
    })
    
    monitor.requests[TEST_SCRAPER].append({
        'timestamp': old_time,
        'success': True,
        'response_time': 0.1,
        'context': {}
    })
    
    # Add new data
    monitor.record_error(TEST_SCRAPER, TEST_ERROR)
    monitor.record_request(TEST_SCRAPER, True, 0.1)
    
    # Verify old data is trimmed
    assert len(monitor.errors[TEST_SCRAPER]) == 1
    assert len(monitor.requests[TEST_SCRAPER]) == 1
    
@pytest.mark.asyncio
async def test_circuit_breaker_basic(circuit_breaker):
    """Test basic circuit breaker functionality"""
    # Test successful operation
    operation = AsyncMock(return_value="success")
    result = await circuit_breaker.execute(operation)
    assert result == "success"
    assert circuit_breaker.state == CircuitState.CLOSED
    
    # Test operation with fallback
    failing_operation = AsyncMock(side_effect=Exception("fail"))
    fallback = AsyncMock(return_value="fallback")
    
    result = await circuit_breaker.execute(failing_operation, fallback)
    assert result == "fallback"
    
@pytest.mark.asyncio
async def test_circuit_breaker_open(circuit_breaker):
    """Test circuit breaker opening on failures"""
    operation = AsyncMock(side_effect=Exception("fail"))
    
    # Cause failures to open circuit
    for _ in range(3):
        with pytest.raises(CircuitBreakerError):
            await circuit_breaker.execute(operation)
            
    assert circuit_breaker.state == CircuitState.OPEN
    
    # Verify circuit blocks operations when open
    with pytest.raises(CircuitBreakerError) as exc_info:
        await circuit_breaker.execute(AsyncMock())
    assert "Circuit breaker is open" in str(exc_info.value)
    
@pytest.mark.asyncio
async def test_circuit_breaker_recovery(circuit_breaker):
    """Test circuit breaker recovery process"""
    # Open the circuit
    operation = AsyncMock(side_effect=Exception("fail"))
    for _ in range(3):
        with pytest.raises(CircuitBreakerError):
            await circuit_breaker.execute(operation)
            
    assert circuit_breaker.state == CircuitState.OPEN
    
    # Wait for recovery timeout
    await asyncio.sleep(1.1)
    
    # Test successful recovery
    success_operation = AsyncMock(return_value="success")
    result = await circuit_breaker.execute(success_operation)
    assert result == "success"
    assert circuit_breaker.state == CircuitState.HALF_OPEN
    
    # Complete recovery with successful operations
    result = await circuit_breaker.execute(success_operation)
    assert result == "success"
    assert circuit_breaker.state == CircuitState.CLOSED
    
@pytest.mark.asyncio
async def test_circuit_breaker_half_open_failure(circuit_breaker):
    """Test circuit breaker handling failure in half-open state"""
    # Open the circuit
    operation = AsyncMock(side_effect=Exception("fail"))
    for _ in range(3):
        with pytest.raises(CircuitBreakerError):
            await circuit_breaker.execute(operation)
            
    # Wait for recovery timeout
    await asyncio.sleep(1.1)
    
    # Fail in half-open state
    with pytest.raises(CircuitBreakerError):
        await circuit_breaker.execute(operation)
        
    # Verify circuit is open again
    assert circuit_breaker.state == CircuitState.OPEN
    
def test_circuit_breaker_metrics(circuit_breaker):
    """Test circuit breaker metrics"""
    metrics = circuit_breaker.get_metrics()
    assert metrics['state'] == CircuitState.CLOSED.value
    assert metrics['failure_count'] == 0
    assert metrics['half_open_successes'] == 0
    assert metrics['last_failure'] is None 