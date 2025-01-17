import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

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
        response = await test_client.post(
            '/api/events/process',
            json={'event': test_event}
        )
        assert response.status_code == 200
        
        # Verify each processing stage
        async with test_db._pool.acquire() as conn:
            processed_event = await conn.fetchrow(
                'SELECT * FROM events WHERE id = $1',
                test_event['id']
            )
            
            # Verify location processing
            assert processed_event['location_coordinates'] is not None
            assert processed_event['location_coordinates']['lat'] == 40.7128
            
            # Verify category normalization
            assert 'music' in processed_event['categories']
            assert 'outdoor' in processed_event['categories']
            
            # Verify price normalization
            assert processed_event['price_range']['min'] == 50
            assert processed_event['price_range']['max'] == 100

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
        response = await test_client.post(
            '/api/events/process',
            json={'event': test_event}
        )
        assert response.status_code == 200
        
        # Verify processing succeeded after retries
        async with test_db._pool.acquire() as conn:
            processed_event = await conn.fetchrow(
                'SELECT * FROM events WHERE id = $1',
                test_event['id']
            )
            assert processed_event is not None
            assert processed_event['location_coordinates'] is not None
            
            # Verify retry attempts were logged
            retry_logs = await conn.fetch(
                'SELECT * FROM processing_logs WHERE event_id = $1 ORDER BY timestamp',
                test_event['id']
            )
            assert len(retry_logs) == 3  # Two failures + one success

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
        for event in test_events:
            response = await test_client.post(
                '/api/events/process',
                json={'event': event}
            )
            assert response.status_code == 200
        
        # Verify data consistency
        async with test_db._pool.acquire() as conn:
            processed_events = await conn.fetch(
                'SELECT * FROM events WHERE series_id = $1',
                'series-A'
            )
            
            # Verify all events in series have consistent location
            locations = [event['location_coordinates'] for event in processed_events]
            assert all(loc == locations[0] for loc in locations)
            
            # Verify series metadata is consistent
            series_meta = await conn.fetchrow(
                'SELECT * FROM event_series WHERE id = $1',
                'series-A'
            )
            assert series_meta['event_count'] == len(test_events)

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
        
        async def process_event(event):
            return await test_client.post(
                '/api/events/process',
                json={'event': event}
            )
        
        responses = await asyncio.gather(
            *[process_event(event) for event in test_events]
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify all events were processed successfully
        assert all(r.status_code == 200 for r in responses)
        
        # Verify processing time is within acceptable range
        assert processing_time < 10.0  # Should process 50 events within 10 seconds
        
        # Verify all events were processed and stored
        async with test_db._pool.acquire() as conn:
            processed_count = await conn.fetchval(
                'SELECT COUNT(*) FROM events WHERE id LIKE $1',
                'event-load-%'
            )
            assert processed_count == 50 