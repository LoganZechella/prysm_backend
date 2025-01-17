import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

class TestEventPipeline:
    async def test_complete_event_processing(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test complete event processing pipeline"""
        # Setup test events
        test_events = [
            {
                'id': f'event-{i}',
                'title': f'Test Event {i}',
                'description': f'Test Description {i}',
                'start_date': datetime.now() + timedelta(days=i),
                'location': {
                    'address': f'Test Location {i}',
                    'coordinates': {'lat': 40.7128, 'lng': -74.0060}
                }
            }
            for i in range(3)
        ]
        
        # Configure mock services
        mock_services['event_source'].set_response('get_events', test_events)
        mock_services['location'].set_response('geocode_Test Location 0', 
            {'lat': 40.7128, 'lng': -74.0060})
        
        # Trigger event processing
        response = await test_client.post('/api/events/process')
        assert response.status_code == 200
        
        # Verify events were processed and stored
        async with test_db._pool.acquire() as conn:
            stored_events = await conn.fetch(
                'SELECT * FROM events WHERE id = ANY($1)',
                [e['id'] for e in test_events]
            )
            assert len(stored_events) == len(test_events)
            
            # Verify event enrichment
            for event in stored_events:
                assert event['processed_timestamp'] is not None
                assert event['location_coordinates'] is not None

    async def test_error_recovery(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test error recovery in event pipeline"""
        # Setup test events with one causing an error
        test_events = [
            {'id': 'event-1', 'title': 'Good Event 1', 'description': 'Test 1'},
            {'id': 'event-2', 'title': None, 'description': 'Should Fail'},  # Will cause error
            {'id': 'event-3', 'title': 'Good Event 2', 'description': 'Test 2'}
        ]
        
        mock_services['event_source'].set_response('get_events', test_events)
        
        # Trigger event processing
        response = await test_client.post('/api/events/process')
        assert response.status_code == 200
        
        # Verify good events were processed despite error
        async with test_db._pool.acquire() as conn:
            stored_events = await conn.fetch(
                'SELECT * FROM events WHERE id IN ($1, $2)',
                'event-1', 'event-3'
            )
            assert len(stored_events) == 2
            
            # Verify error was logged
            error_logs = await conn.fetch(
                'SELECT * FROM error_logs WHERE event_id = $1',
                'event-2'
            )
            assert len(error_logs) == 1
            assert 'title' in error_logs[0]['error_message']

    async def test_duplicate_handling(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test handling of duplicate events"""
        # Setup test event that will be duplicated
        test_event = {
            'id': 'event-dup',
            'title': 'Duplicate Event',
            'description': 'This event will be processed twice',
            'start_date': datetime.now() + timedelta(days=1),
            'location': {
                'address': 'Test Location',
                'coordinates': {'lat': 40.7128, 'lng': -74.0060}
            }
        }
        
        # First processing
        mock_services['event_source'].set_response('get_events', [test_event])
        response = await test_client.post('/api/events/process')
        assert response.status_code == 200
        
        # Second processing of same event
        response = await test_client.post('/api/events/process')
        assert response.status_code == 200
        
        # Verify event was stored only once
        async with test_db._pool.acquire() as conn:
            stored_events = await conn.fetch(
                'SELECT * FROM events WHERE id = $1',
                test_event['id']
            )
            assert len(stored_events) == 1
            
            # Verify no error logs for duplicate
            error_logs = await conn.fetch(
                'SELECT * FROM error_logs WHERE event_id = $1',
                test_event['id']
            )
            assert len(error_logs) == 0 