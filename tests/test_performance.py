import pytest
import asyncio
import time
from app.monitoring.performance import PerformanceBenchmark, ConcurrencyTester

class TestPerformanceBenchmarking:
    @pytest.fixture
    def benchmark(self):
        return PerformanceBenchmark(
            max_workers=5,
            warmup_seconds=1,
            cooldown_seconds=1
        )
        
    @pytest.mark.asyncio
    async def test_load_test_basic(self, benchmark):
        """Test basic load testing functionality"""
        async def test_func():
            await asyncio.sleep(0.1)  # Simulate work
            return True
            
        result = await benchmark.load_test(
            operation="test_op",
            test_func=test_func,
            requests_per_second=10,
            duration_seconds=2
        )
        
        assert result.operation == "test_op"
        assert result.total_time >= 2.0  # At least duration_seconds
        assert result.requests_per_second > 0
        assert result.mean_latency > 0
        assert result.error_rate == 0
        
    @pytest.mark.asyncio
    async def test_load_test_with_errors(self, benchmark):
        """Test load testing with failing requests"""
        async def failing_func():
            await asyncio.sleep(0.1)
            if time.time() % 2 > 1:  # Fail randomly
                raise Exception("Random failure")
            return True
            
        result = await benchmark.load_test(
            operation="failing_op",
            test_func=failing_func,
            requests_per_second=10,
            duration_seconds=2
        )
        
        assert result.operation == "failing_op"
        assert result.error_rate > 0
        
    def test_measure_time(self, benchmark):
        """Test operation timing measurement"""
        with benchmark.measure_time("test_operation"):
            time.sleep(0.1)  # Simulate work
            
    def test_benchmark_history(self, benchmark):
        """Test benchmark history tracking"""
        # Add some test results
        result = benchmark.BenchmarkResult(
            operation="test_op",
            total_time=1.0,
            requests_per_second=10.0,
            mean_latency=0.1,
            p95_latency=0.15,
            p99_latency=0.2,
            error_rate=0.0,
            timestamp=time.time(),
            details={}
        )
        
        benchmark.results["test_op"] = [result]
        
        # Test getting history
        history = benchmark.get_benchmark_history("test_op")
        assert "test_op" in history
        assert len(history["test_op"]) == 1
        
        # Test getting all history
        all_history = benchmark.get_benchmark_history()
        assert "test_op" in all_history
        
        # Test clearing history
        benchmark.clear_history()
        assert not benchmark.get_benchmark_history()
        
class TestConcurrencyTesting:
    @pytest.fixture
    def concurrency_tester(self):
        tester = ConcurrencyTester(max_workers=5)
        yield tester
        tester.cleanup()
        
    @pytest.mark.asyncio
    async def test_concurrent_access(self, concurrency_tester):
        """Test concurrent access handling"""
        def test_func():
            time.sleep(0.1)  # Simulate work
            return True
            
        result = await concurrency_tester.test_concurrent_access(
            test_func=test_func,
            num_requests=10
        )
        
        assert result['successful_requests'] == 10
        assert result['failed_requests'] == 0
        assert result['requests_per_second'] > 0
        assert result['error_rate'] == 0
        
    @pytest.mark.asyncio
    async def test_concurrent_access_with_errors(self, concurrency_tester):
        """Test concurrent access with errors"""
        def failing_func():
            time.sleep(0.1)
            if time.time() % 2 > 1:  # Fail randomly
                raise Exception("Random failure")
            return True
            
        result = await concurrency_tester.test_concurrent_access(
            test_func=failing_func,
            num_requests=10
        )
        
        assert result['successful_requests'] < 10
        assert result['failed_requests'] > 0
        assert result['error_rate'] > 0
        
    @pytest.mark.asyncio
    async def test_concurrent_access_resource_cleanup(self, concurrency_tester):
        """Test resource cleanup after concurrent access"""
        def test_func():
            time.sleep(0.1)
            return True
            
        await concurrency_tester.test_concurrent_access(
            test_func=test_func,
            num_requests=5
        )
        
        # Cleanup should not raise any errors
        concurrency_tester.cleanup()
        
    @pytest.mark.asyncio
    async def test_max_workers_limit(self, concurrency_tester):
        """Test max workers limit is respected"""
        start_time = time.time()
        
        def slow_func():
            time.sleep(0.5)  # Each request takes 0.5s
            return True
            
        result = await concurrency_tester.test_concurrent_access(
            test_func=slow_func,
            num_requests=10  # With 5 workers, should take at least 1s
        )
        
        duration = time.time() - start_time
        assert duration >= 1.0  # Should take at least 2 batches of 0.5s each 