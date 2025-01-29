"""
Performance benchmarking and load testing utilities for the recommendation system.
"""

import time
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable, Coroutine
import logging
import statistics
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """Container for benchmark results"""
    operation: str
    total_time: float
    requests_per_second: float
    mean_latency: float
    p95_latency: float
    p99_latency: float
    error_rate: float
    timestamp: datetime
    details: Dict[str, Any]

class PerformanceBenchmark:
    """Performance benchmarking and load testing utility"""
    
    def __init__(
        self,
        max_workers: int = 10,
        warmup_seconds: int = 5,
        cooldown_seconds: int = 5
    ):
        self.max_workers = max_workers
        self.warmup_seconds = warmup_seconds
        self.cooldown_seconds = cooldown_seconds
        self.results: Dict[str, List[BenchmarkResult]] = {}
        
    @contextmanager
    def measure_time(self, operation: str) -> float:
        """Context manager to measure operation time"""
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            logger.info(
                f"Operation timing: {operation}",
                extra={
                    'operation': operation,
                    'duration': duration
                }
            )
            
    async def load_test(
        self,
        operation: str,
        test_func: Callable[..., Coroutine[Any, Any, Any]],
        requests_per_second: int,
        duration_seconds: int,
        **kwargs
    ) -> BenchmarkResult:
        """
        Run a load test with specified RPS.
        
        Args:
            operation: Name of operation being tested
            test_func: Async function to test
            requests_per_second: Target requests per second
            duration_seconds: How long to run the test
            **kwargs: Arguments to pass to test_func
            
        Returns:
            BenchmarkResult with test metrics
        """
        logger.info(
            f"Starting load test for {operation}",
            extra={
                'operation': operation,
                'rps': requests_per_second,
                'duration': duration_seconds
            }
        )
        
        # Calculate delay between requests to achieve target RPS
        delay = 1.0 / requests_per_second
        
        # Prepare result tracking
        latencies: List[float] = []
        errors = 0
        total_requests = 0
        
        # Warmup phase
        logger.info(f"Warming up for {self.warmup_seconds} seconds")
        await asyncio.sleep(self.warmup_seconds)
        
        # Main test phase
        start_time = time.time()
        tasks = []
        
        async def _run_request():
            try:
                request_start = time.time()
                await test_func(**kwargs)
                latency = time.time() - request_start
                latencies.append(latency)
            except Exception as e:
                errors += 1
                logger.error(
                    f"Request error in {operation}",
                    exc_info=e,
                    extra={'operation': operation}
                )
                
        while time.time() - start_time < duration_seconds:
            if len(asyncio.all_tasks()) - 1 < self.max_workers:  # -1 for current task
                tasks.append(asyncio.create_task(_run_request()))
                total_requests += 1
                await asyncio.sleep(delay)
                
        # Wait for remaining tasks
        if tasks:
            await asyncio.gather(*tasks)
            
        # Cooldown phase
        logger.info(f"Cooling down for {self.cooldown_seconds} seconds")
        await asyncio.sleep(self.cooldown_seconds)
        
        # Calculate metrics
        total_time = time.time() - start_time
        actual_rps = total_requests / total_time
        
        if latencies:
            mean_latency = statistics.mean(latencies)
            sorted_latencies = sorted(latencies)
            p95_index = int(len(sorted_latencies) * 0.95)
            p99_index = int(len(sorted_latencies) * 0.99)
            p95_latency = sorted_latencies[p95_index]
            p99_latency = sorted_latencies[p99_index]
        else:
            mean_latency = p95_latency = p99_latency = 0
            
        error_rate = errors / total_requests if total_requests > 0 else 0
        
        result = BenchmarkResult(
            operation=operation,
            total_time=total_time,
            requests_per_second=actual_rps,
            mean_latency=mean_latency,
            p95_latency=p95_latency,
            p99_latency=p99_latency,
            error_rate=error_rate,
            timestamp=datetime.utcnow(),
            details={
                'target_rps': requests_per_second,
                'total_requests': total_requests,
                'errors': errors,
                'concurrent_workers': self.max_workers
            }
        )
        
        if operation not in self.results:
            self.results[operation] = []
        self.results[operation].append(result)
        
        logger.info(
            f"Load test completed for {operation}",
            extra={
                'operation': operation,
                'actual_rps': actual_rps,
                'mean_latency': mean_latency,
                'p95_latency': p95_latency,
                'error_rate': error_rate
            }
        )
        
        return result
        
    def get_benchmark_history(
        self,
        operation: Optional[str] = None
    ) -> Dict[str, List[BenchmarkResult]]:
        """Get benchmark history for specified operation or all operations"""
        if operation:
            return {operation: self.results.get(operation, [])}
        return self.results
        
    def clear_history(self) -> None:
        """Clear benchmark history"""
        self.results.clear()
        
class ConcurrencyTester:
    """Utility for testing system behavior under concurrent load"""
    
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
    async def test_concurrent_access(
        self,
        test_func: Callable[..., Any],
        num_requests: int,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Test function behavior under concurrent access.
        
        Args:
            test_func: Function to test
            num_requests: Number of concurrent requests
            **kwargs: Arguments to pass to test_func
            
        Returns:
            Dictionary with test results
        """
        start_time = time.time()
        results = []
        errors = []
        
        def _run_with_error_tracking():
            try:
                return test_func(**kwargs)
            except Exception as e:
                errors.append(e)
                logger.error("Concurrent access error", exc_info=e)
                return None
                
        # Submit all requests
        futures = [
            self.executor.submit(_run_with_error_tracking)
            for _ in range(num_requests)
        ]
        
        # Gather results
        for future in futures:
            try:
                result = future.result()
                if result is not None:
                    results.append(result)
            except Exception as e:
                errors.append(e)
                
        total_time = time.time() - start_time
        
        return {
            'total_time': total_time,
            'successful_requests': len(results),
            'failed_requests': len(errors),
            'requests_per_second': num_requests / total_time,
            'error_rate': len(errors) / num_requests if num_requests > 0 else 0
        }
        
    def cleanup(self):
        """Cleanup resources"""
        self.executor.shutdown(wait=True)

class PerformanceMonitor(PerformanceBenchmark):
    """
    Legacy compatibility class that provides the old PerformanceMonitor interface.
    This class inherits from PerformanceBenchmark to maintain backward compatibility
    while providing access to new functionality.
    """
    
    def __init__(self):
        super().__init__()
        self.metrics = {
            'api_calls': {},
            'errors': [],
            'last_error_time': None
        }
        
    @contextmanager
    def monitor_api_call(self, operation_name: str):
        """Monitor API call duration and status"""
        with self.measure_time(operation_name) as duration:
            try:
                yield
            finally:
                if operation_name not in self.metrics['api_calls']:
                    self.metrics['api_calls'][operation_name] = {
                        'count': 0,
                        'total_duration': 0,
                        'errors': 0,
                        'last_call_time': None
                    }
                    
                metrics = self.metrics['api_calls'][operation_name]
                metrics['count'] += 1
                metrics['total_duration'] += duration
                metrics['last_call_time'] = datetime.utcnow().isoformat()
                
    def record_error(self, operation: str, error_message: str):
        """Record an error occurrence"""
        error_time = datetime.utcnow()
        error_entry = {
            'operation': operation,
            'message': error_message,
            'timestamp': error_time.isoformat()
        }
        
        self.metrics['errors'].append(error_entry)
        self.metrics['last_error_time'] = error_time
        
        logger.error(f"Operation '{operation}' failed: {error_message}")
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        return self.metrics
        
    def reset_metrics(self):
        """Reset all collected metrics"""
        self.metrics = {
            'api_calls': {},
            'errors': [],
            'last_error_time': None
        } 