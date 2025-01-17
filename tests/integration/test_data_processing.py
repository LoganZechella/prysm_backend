import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

@pytest.mark.asyncio
class TestDataProcessing:
    async def test_multi_stage_processing(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test complete data processing pipeline with multiple stages"""
        # Setup test event
        test_event = {
            'id': 'event-pipeline-1',
            'title': 'Test Pipeline Event',
            'description': 'Event for testing pipeline processing',
            'raw_location': '123 Main St, New York, NY',
            'raw_categories': ['Live Music', 'Outdoor Event', 'Family Friendly'],
            'raw_price': '$50-$100'
        }
        
        # Configure mock services
        mock_services['location'].set_response(
            'geocode_123 Main St, New York, NY',
            {'lat': 40.7128, 'lng': -74.0060}
        )
        
        # Trigger processing
        response = test_client.post(
            '/api/events/process',
            json={'event': test_event}
        )
        assert response.status_code == 200
        processed_event = response.json()
        
        # Verify location coordinates
        assert processed_event['location']['coordinates'] == {'lat': 40.7128, 'lng': -74.0060}
        
        # Verify category normalization
        assert 'music' in processed_event['categories']
        assert 'outdoor' in processed_event['categories']
        assert 'family' in processed_event['categories']
        
        # Verify price normalization
        assert processed_event['price_info']['min'] == 50
        assert processed_event['price_info']['max'] == 100

    async def test_error_recovery_and_retry(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test error recovery and retry mechanisms in processing pipeline"""
        test_event = {
            'id': 'event-retry-1',
            'title': 'Retry Test Event',
            'raw_location': 'Test Location'
        }
        
        # Configure location service to fail twice then succeed
        fail_count = 0
        async def geocode_with_retry(address: str):
            nonlocal fail_count
            fail_count += 1
            if fail_count <= 2:
                raise Exception('Temporary failure')
            return {'lat': 40.7128, 'lng': -74.0060}
        
        mock_services['location'].geocode = geocode_with_retry
        
        # Trigger processing
        response = test_client.post(
            '/api/events/process',
            json={'event': test_event}
        )
        assert response.status_code == 200
        processed_event = response.json()
        
        # Verify retry logs
        assert fail_count == 3  # Two failures + one success
        assert processed_event['location']['coordinates'] == {'lat': 40.7128, 'lng': -74.0060}

    async def test_data_consistency(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test data consistency across processing stages"""
        # Setup test events with related data
        test_events = [
            {
                'id': 'event-series-1',
                'title': 'Series Event 1',
                'series_id': 'series-A',
                'raw_location': 'Series Venue'
            },
            {
                'id': 'event-series-2',
                'title': 'Series Event 2',
                'series_id': 'series-A',
                'raw_location': 'Series Venue'
            }
        ]
        
        # Configure mock services
        mock_services['location'].set_response(
            'geocode_Series Venue',
            {'lat': 40.7128, 'lng': -74.0060}
        )
        
        # Process events
        processed_events = []
        for event in test_events:
            response = test_client.post(
                '/api/events/process',
                json={'event': event}
            )
            assert response.status_code == 200
            processed_events.append(response.json())
        
        # Verify location consistency
        assert all(
            event['location']['coordinates'] == {'lat': 40.7128, 'lng': -74.0060}
            for event in processed_events
        )
        
        # Verify series metadata
        assert all(
            event['series_id'] == 'series-A'
            for event in processed_events
        )

    async def test_performance_under_load(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test processing pipeline performance under load"""
        # Generate batch of test events
        test_events = [
            {
                'id': f'event-load-{i}',
                'title': f'Load Test Event {i}',
                'raw_location': f'Location {i}',
                'raw_categories': ['Test Category'],
                'raw_price': '$100'
            }
            for i in range(50)  # Test with 50 concurrent events
        ]
        
        # Configure mock services with fast responses
        for i in range(50):
            mock_services['location'].set_response(
                f'geocode_Location {i}',
                {'lat': 40.7128, 'lng': -74.0060}
            )
        
        # Process events concurrently
        import asyncio
        import time
        
        start_time = time.time()
        
        # Process events sequentially for now
        responses = []
        for event in test_events:
            response = test_client.post(
                '/api/events/process',
                json={'event': event}
            )
            assert response.status_code == 200
            responses.append(response.json())
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify all events were processed
        assert len(responses) == 50
        assert all(
            event['location']['coordinates'] == {'lat': 40.7128, 'lng': -74.0060}
            for event in responses
        )
        
        # Verify processing time is within acceptable range (adjust as needed)
        assert processing_time < 10  # Should process 50 events in under 10 seconds 