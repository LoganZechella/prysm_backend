import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

@pytest.mark.asyncio
class TestEventPipeline:
    async def test_complete_event_processing(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test complete event processing pipeline"""
        # Setup test event
        test_event = {
            'id': 'test-event-1',
            'title': 'Test Event',
            'description': 'A test event for pipeline processing',
            'raw_location': '123 Test St, Test City',
            'raw_categories': ['Music', 'Outdoor'],
            'raw_price': '$20-30'
        }

        # Configure mock services
        mock_services['location'].set_response(
            'geocode_123 Test St, Test City',
            {'lat': 40.7128, 'lng': -74.0060}
        )

        # Process event
        response = test_client.post(
            '/api/events/process',
            json={'event': test_event}
        )
        assert response.status_code == 200
        processed_event = response.json()

        # Verify event was processed correctly
        assert processed_event['id'] == test_event['id']
        assert processed_event['title'] == test_event['title']
        assert processed_event['description'] == test_event['description']
        assert processed_event['location']['coordinates'] == {'lat': 40.7128, 'lng': -74.0060}
        assert 'music' in processed_event['categories']
        assert 'outdoor' in processed_event['categories']
        assert processed_event['price_info']['min'] == 20
        assert processed_event['price_info']['max'] == 30

    async def test_batch_event_processing(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test batch event processing"""
        # Setup batch of test events
        test_events = [
            {
                'id': f'batch-event-{i}',
                'title': f'Batch Event {i}',
                'raw_location': f'Location {i}',
                'raw_categories': ['Test Category'],
                'raw_price': '$50'
            }
            for i in range(5)
        ]

        # Configure mock services
        for i in range(5):
            mock_services['location'].set_response(
                f'geocode_Location {i}',
                {'lat': 40.7128, 'lng': -74.0060}
            )

        # Process batch
        response = test_client.post(
            '/api/events/batch_process',
            json={'events': test_events}
        )
        assert response.status_code == 200
        processed_events = response.json()['events']

        # Verify all events were processed
        assert len(processed_events) == 5
        for i, event in enumerate(processed_events):
            assert event['id'] == f'batch-event-{i}'
            assert event['location']['coordinates'] == {'lat': 40.7128, 'lng': -74.0060}
            assert 'test' in event['categories']
            assert event['price_info']['min'] == 50
            assert event['price_info']['max'] == 50

    async def test_event_deduplication(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test event deduplication in pipeline"""
        # Setup duplicate events
        test_event = {
            'id': 'dupe-event-1',
            'source': 'test_source',
            'source_id': 'original_id',
            'title': 'Duplicate Event',
            'raw_location': 'Dupe Location'
        }

        # Configure mock services
        mock_services['location'].set_response(
            'geocode_Dupe Location',
            {'lat': 40.7128, 'lng': -74.0060}
        )

        # Process original event
        response = test_client.post(
            '/api/events/process',
            json={'event': test_event}
        )
        assert response.status_code == 200
        original_event = response.json()

        # Attempt to process duplicate
        duplicate_event = test_event.copy()
        duplicate_event['id'] = 'dupe-event-2'
        response = test_client.post(
            '/api/events/process',
            json={'event': duplicate_event}
        )
        assert response.status_code == 200
        processed_duplicate = response.json()

        # Verify events were deduplicated
        assert processed_duplicate['duplicate_of'] == original_event['id']
        assert processed_duplicate['source'] == test_event['source']
        assert processed_duplicate['source_id'] == test_event['source_id'] 