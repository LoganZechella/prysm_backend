import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List
import time

@pytest.mark.asyncio
class TestMonitoring:
    async def test_metric_collection(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test collection and reporting of metrics"""
        # Setup test event
        event = {
            'id': 'metric-test-1',
            'title': 'Metric Test Event',
            'raw_location': 'Test Location',
            'raw_categories': ['Test Category'],
            'raw_price': '$50'
        }

        # Configure mock services
        mock_services['location'].set_response(
            'geocode_Test Location',
            {'lat': 40.7128, 'lng': -74.0060}
        )

        # Process event to generate metrics
        response = test_client.post(
            '/api/events/process',
            json={'event': event}
        )
        assert response.status_code == 200

        # Check metrics endpoint
        response = test_client.get('/metrics')
        assert response.status_code == 200
        metrics = response.text

        # Verify expected metrics are present
        assert 'events_processed_total' in metrics
        assert 'event_processing_duration_seconds' in metrics
        assert 'location_service_requests_total' in metrics
        assert 'database_operations_total' in metrics

    async def test_trace_sampling(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test trace sampling and distributed tracing"""
        # Setup test event with trace context
        event = {
            'id': 'trace-test-1',
            'title': 'Trace Test Event',
            'raw_location': 'Test Location'
        }

        # Configure mock services
        mock_services['location'].set_response(
            'geocode_Test Location',
            {'lat': 40.7128, 'lng': -74.0060}
        )

        # Process event with trace headers
        headers = {
            'X-B3-TraceId': 'test-trace-id',
            'X-B3-SpanId': 'test-span-id',
            'X-B3-Sampled': '1'
        }
        response = test_client.post(
            '/api/events/process',
            json={'event': event},
            headers=headers
        )
        assert response.status_code == 200

        # Verify trace data was collected
        response = test_client.get('/traces/test-trace-id')
        assert response.status_code == 200
        trace_data = response.json()

        # Verify expected spans in trace
        assert len(trace_data['spans']) > 0
        assert any(span['name'] == 'process_event' for span in trace_data['spans'])
        assert any(span['name'] == 'geocode_location' for span in trace_data['spans'])

    async def test_error_tracking(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test error tracking and reporting"""
        # Setup test event that will cause an error
        event = {
            'id': 'error-test-1',
            'title': 'Error Test Event',
            'raw_location': None  # This will cause a validation error
        }

        # Process event to generate error
        response = test_client.post(
            '/api/events/process',
            json={'event': event}
        )
        assert response.status_code == 422  # Validation error

        # Check error metrics
        response = test_client.get('/metrics')
        assert response.status_code == 200
        metrics = response.text

        # Verify error metrics
        assert 'validation_errors_total' in metrics
        assert 'error_events_total{type="validation"}' in metrics

        # Check error logs
        response = test_client.get('/errors')
        assert response.status_code == 200
        errors = response.json()

        # Verify error was logged
        assert len(errors) > 0
        assert any(
            error['event_id'] == 'error-test-1' and
            error['error_type'] == 'ValidationError'
            for error in errors
        )

    async def test_performance_monitoring(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test performance monitoring and alerting"""
        # Generate load with multiple events
        events = [
            {
                'id': f'perf-test-{i}',
                'title': f'Performance Test Event {i}',
                'raw_location': 'Test Location',
                'raw_categories': ['Test Category'],
                'raw_price': '$50'
            }
            for i in range(10)
        ]

        # Configure mock services
        mock_services['location'].set_response(
            'geocode_Test Location',
            {'lat': 40.7128, 'lng': -74.0060}
        )

        # Process events to generate performance metrics
        for event in events:
            response = test_client.post(
                '/api/events/process',
                json={'event': event}
            )
            assert response.status_code == 200

        # Check performance metrics
        response = test_client.get('/metrics')
        assert response.status_code == 200
        metrics = response.text

        # Verify performance metrics
        assert 'event_processing_duration_seconds' in metrics
        assert 'event_processing_queue_length' in metrics
        assert 'database_operation_duration_seconds' in metrics

    async def test_health_checks(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test health check endpoints"""
        # Check basic health endpoint
        response = test_client.get('/health')
        assert response.status_code == 200
        health_data = response.json()
        assert health_data['status'] == 'healthy'

        # Check detailed health status
        response = test_client.get('/health/details')
        assert response.status_code == 200
        details = response.json()

        # Verify component health status
        assert 'database' in details
        assert details['database']['status'] == 'healthy'
        assert 'event_processing' in details
        assert details['event_processing']['status'] == 'healthy'
        assert 'location_service' in details
        assert details['location_service']['status'] == 'healthy'

        # Verify metrics in health check
        assert 'metrics' in details
        assert 'event_processing_success_rate' in details['metrics']
        assert 'average_processing_time' in details['metrics'] 