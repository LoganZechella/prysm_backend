import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List
import time

class TestMonitoring:
    async def test_metric_collection(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test metric collection and reporting"""
        # Process a batch of events to generate metrics
        test_events = [
            {
                'id': f'event-metric-{i}',
                'title': f'Metric Test Event {i}',
                'description': f'Test Description {i}'
            }
            for i in range(5)
        ]
        
        for event in test_events:
            response = await test_client.post(
                '/api/events/process',
                json={'event': event}
            )
            assert response.status_code == 200
        
        # Get metrics
        response = await test_client.get('/metrics')
        assert response.status_code == 200
        metrics = response.text
        
        # Verify expected metrics are present
        assert 'event_processing_duration_seconds' in metrics
        assert 'event_processing_total' in metrics
        assert 'event_processing_errors_total' in metrics
        
        # Verify metric values
        assert 'event_processing_total{status="success"} 5' in metrics

    async def test_trace_sampling(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test trace sampling and distributed tracing"""
        # Configure trace sampling
        headers = {'X-Trace-Enabled': 'true'}
        
        # Execute traced operation
        response = await test_client.post(
            '/api/events/process',
            headers=headers,
            json={
                'event': {
                    'id': 'event-trace-1',
                    'title': 'Trace Test Event'
                }
            }
        )
        assert response.status_code == 200
        
        # Verify trace headers in response
        assert 'X-Trace-ID' in response.headers
        trace_id = response.headers['X-Trace-ID']
        
        # Get trace data
        response = await test_client.get(f'/traces/{trace_id}')
        assert response.status_code == 200
        trace_data = response.json()
        
        # Verify trace contains expected spans
        spans = trace_data['spans']
        span_names = [span['name'] for span in spans]
        assert 'event_processing' in span_names
        assert 'database_operation' in span_names
        assert 'external_service_call' in span_names

    async def test_error_tracking(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test error tracking and reporting"""
        # Trigger an error condition
        response = await test_client.post(
            '/api/events/process',
            json={
                'event': {
                    'id': 'event-error-1',
                    'title': None  # This should cause an error
                }
            }
        )
        assert response.status_code == 400
        
        # Get error metrics
        response = await test_client.get('/metrics')
        assert response.status_code == 200
        metrics = response.text
        
        # Verify error metrics
        assert 'event_processing_errors_total{error_type="validation"} 1' in metrics
        
        # Get error logs
        response = await test_client.get('/api/logs/errors')
        assert response.status_code == 200
        error_logs = response.json()
        
        # Verify error was logged
        assert any(
            log['event_id'] == 'event-error-1' and
            log['error_type'] == 'validation'
            for log in error_logs
        )

    async def test_performance_monitoring(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test performance monitoring and alerting"""
        # Generate load
        test_events = [
            {
                'id': f'event-perf-{i}',
                'title': f'Performance Test Event {i}'
            }
            for i in range(20)
        ]
        
        start_time = time.time()
        
        # Process events
        import asyncio
        responses = await asyncio.gather(*[
            test_client.post('/api/events/process', json={'event': event})
            for event in test_events
        ])
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Get performance metrics
        response = await test_client.get('/metrics')
        assert response.status_code == 200
        metrics = response.text
        
        # Verify performance metrics
        assert 'http_request_duration_seconds' in metrics
        assert 'http_requests_total' in metrics
        assert 'process_cpu_seconds_total' in metrics
        assert 'process_memory_bytes' in metrics
        
        # Verify processing time is within acceptable range
        assert processing_time < 10.0  # Should process 20 events within 10 seconds

    async def test_health_checks(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test health check endpoints and monitoring"""
        # Test basic health check
        response = await test_client.get('/health')
        assert response.status_code == 200
        health_data = response.json()
        assert health_data['status'] == 'healthy'
        
        # Test detailed health check
        response = await test_client.get('/health/detailed')
        assert response.status_code == 200
        detailed_health = response.json()
        
        # Verify component health status
        assert detailed_health['database']['status'] == 'healthy'
        assert detailed_health['cache']['status'] == 'healthy'
        assert detailed_health['external_services']['status'] == 'healthy'
        
        # Verify metrics
        assert 'uptime_seconds' in detailed_health
        assert 'connection_pool_used' in detailed_health['database']
        assert 'cache_hit_ratio' in detailed_health['cache'] 