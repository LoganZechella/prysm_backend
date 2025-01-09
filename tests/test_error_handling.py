"""
Tests for error handling, rate limiting, and retry functionality.
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch
import aiohttp

from app.utils.rate_limiter import RateLimiter
from app.utils.retry_handler import RetryHandler
from app.scrapers.base import (
    ScrapflyBaseScraper,
    ScrapflyError,
    ScrapflyRateLimitError,
    ScrapflyResponseError
)

# Test data
TEST_URL = "https://test.com/event"
TEST_API_KEY = "test_key"

@pytest.fixture
def rate_limiter():
    """Create a RateLimiter instance for testing"""
    return RateLimiter(requests_per_second=10.0)

@pytest.fixture
def retry_handler():
    """Create a RetryHandler instance for testing"""
    return RetryHandler(max_retries=3)

@pytest.fixture
def base_scraper():
    """Create a ScrapflyBaseScraper instance for testing"""
    return ScrapflyBaseScraper(TEST_API_KEY)

@pytest.mark.asyncio
async def test_rate_limiter_basic(rate_limiter):
    """Test basic rate limiting functionality"""
    # Should allow burst up to burst limit
    for _ in range(20):  # Burst limit is 2x requests_per_second
        assert await rate_limiter.acquire() is True
        
    # Get current stats
    stats = rate_limiter.get_stats()
    assert stats['total_requests'] == 20
    assert stats['delayed_requests'] >= 0
    
@pytest.mark.asyncio
async def test_rate_limiter_delay(rate_limiter):
    """Test rate limiter delay when limit is exceeded"""
    # Use up burst allowance
    for _ in range(20):
        await rate_limiter.acquire()
        
    # Next request should be delayed
    start_time = datetime.utcnow()
    await rate_limiter.acquire()
    duration = (datetime.utcnow() - start_time).total_seconds()
    
    assert duration >= 0.1  # Should have waited at least 100ms
    
@pytest.mark.asyncio
async def test_retry_handler_success(retry_handler):
    """Test successful operation with retry handler"""
    mock_operation = AsyncMock(return_value="success")
    
    result = await retry_handler.execute_with_retry(mock_operation)
    assert result == "success"
    assert mock_operation.call_count == 1
    
    stats = retry_handler.get_stats()
    assert stats['total_attempts'] == 1
    assert stats['successful_retries'] == 0
    
@pytest.mark.asyncio
async def test_retry_handler_retry_success(retry_handler):
    """Test operation that succeeds after retries"""
    fail_count = 0
    
    async def flaky_operation():
        nonlocal fail_count
        if fail_count < 2:
            fail_count += 1
            raise Exception("Temporary error")
        return "success"
        
    result = await retry_handler.execute_with_retry(flaky_operation)
    assert result == "success"
    
    stats = retry_handler.get_stats()
    assert stats['total_attempts'] == 3
    assert stats['successful_retries'] == 1
    
@pytest.mark.asyncio
async def test_retry_handler_max_retries(retry_handler):
    """Test operation that fails after max retries"""
    mock_operation = AsyncMock(side_effect=Exception("Persistent error"))
    
    with pytest.raises(Exception, match="Persistent error"):
        await retry_handler.execute_with_retry(mock_operation)
        
    assert mock_operation.call_count == retry_handler.max_retries + 1
    
    stats = retry_handler.get_stats()
    assert stats['failed_retries'] == 1
    
@pytest.mark.asyncio
async def test_scraper_rate_limit_error(base_scraper):
    """Test scraper handling of rate limit errors"""
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_response = Mock()
        mock_response.status = 429
        mock_get.return_value.__aenter__.return_value = mock_response
        
        with pytest.raises(ScrapflyRateLimitError):
            await base_scraper._make_request(TEST_URL)
            
@pytest.mark.asyncio
async def test_scraper_retry_success(base_scraper):
    """Test scraper retry on temporary error"""
    fail_count = 0
    
    async def mock_response():
        nonlocal fail_count
        if fail_count < 1:
            fail_count += 1
            raise aiohttp.ClientError()
        return {
            'result': {
                'content': '<html><body>Test content</body></html>'
            }
        }
        
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_response_obj = Mock()
        mock_response_obj.status = 200
        mock_response_obj.json = AsyncMock(side_effect=mock_response)
        mock_get.return_value.__aenter__.return_value = mock_response_obj
        
        result = await base_scraper._make_request(TEST_URL)
        assert result is not None
        assert str(result) == '<html><body>Test content</body></html>'
        
@pytest.mark.asyncio
async def test_scraper_error_response(base_scraper):
    """Test scraper handling of error responses"""
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_response = Mock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_get.return_value.__aenter__.return_value = mock_response
        
        with pytest.raises(ScrapflyResponseError) as exc_info:
            await base_scraper._make_request(TEST_URL)
            
        assert "500" in str(exc_info.value)
        
@pytest.mark.asyncio
async def test_scraper_empty_response(base_scraper):
    """Test scraper handling of empty responses"""
    with patch('aiohttp.ClientSession.get') as mock_get:
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={'result': {}})
        mock_get.return_value.__aenter__.return_value = mock_response
        
        with pytest.raises(ScrapflyResponseError) as exc_info:
            await base_scraper._make_request(TEST_URL)
            
        assert "Empty response" in str(exc_info.value)
        
@pytest.mark.asyncio
async def test_scraper_context_manager(base_scraper):
    """Test scraper as async context manager"""
    async with base_scraper as scraper:
        assert scraper.session is not None
        
    assert scraper.session is None 