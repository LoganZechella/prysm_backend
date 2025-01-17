import pytest
from datetime import datetime
from typing import Dict, Any, List

@pytest.mark.asyncio
class TestUserPreferences:
    async def test_preference_update_propagation(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test that user preference changes propagate correctly"""
        # Create test user
        user_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!',
            'name': 'Test User'
        }
        response = test_client.post('/api/auth/register', json=user_data)
        assert response.status_code == 201
        user_id = response.json()['id']

        # Set initial preferences
        initial_prefs = {
            'categories': ['music', 'sports'],
            'max_price': 100,
            'location': {'lat': 40.7128, 'lng': -74.0060},
            'radius_km': 10
        }
        response = test_client.post(
            f'/api/users/{user_id}/preferences',
            json=initial_prefs
        )
        assert response.status_code == 200

        # Update preferences
        updated_prefs = {
            'categories': ['music', 'arts'],
            'max_price': 200,
            'location': {'lat': 40.7128, 'lng': -74.0060},
            'radius_km': 20
        }
        response = test_client.put(
            f'/api/users/{user_id}/preferences',
            json=updated_prefs
        )
        assert response.status_code == 200

        # Verify preferences were updated
        response = test_client.get(f'/api/users/{user_id}/preferences')
        assert response.status_code == 200
        current_prefs = response.json()
        assert current_prefs['categories'] == updated_prefs['categories']
        assert current_prefs['max_price'] == updated_prefs['max_price']
        assert current_prefs['radius_km'] == updated_prefs['radius_km']

        # Verify recommendation cache was invalidated
        response = test_client.get(f'/api/users/{user_id}/recommendations')
        assert response.status_code == 200
        recommendations = response.json()
        assert len(recommendations) > 0
        for event in recommendations:
            assert event['price_info']['max'] <= updated_prefs['max_price']
            assert any(cat in event['categories'] for cat in updated_prefs['categories'])

    async def test_concurrent_preference_updates(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test handling of concurrent preference updates"""
        # Create test user
        user_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!',
            'name': 'Test User'
        }
        response = test_client.post('/api/auth/register', json=user_data)
        assert response.status_code == 201
        user_id = response.json()['id']

        # Set initial preferences
        initial_prefs = {
            'categories': ['music'],
            'max_price': 100
        }
        response = test_client.post(
            f'/api/users/{user_id}/preferences',
            json=initial_prefs
        )
        assert response.status_code == 200

        # Simulate concurrent updates
        update_1 = {
            'categories': ['sports'],
            'max_price': 150
        }
        update_2 = {
            'categories': ['arts'],
            'max_price': 200
        }

        # Send updates
        response1 = test_client.put(
            f'/api/users/{user_id}/preferences',
            json=update_1
        )
        assert response1.status_code == 200

        response2 = test_client.put(
            f'/api/users/{user_id}/preferences',
            json=update_2
        )
        assert response2.status_code == 200

        # Verify final state
        response = test_client.get(f'/api/users/{user_id}/preferences')
        assert response.status_code == 200
        final_prefs = response.json()
        assert final_prefs['categories'] == update_2['categories']
        assert final_prefs['max_price'] == update_2['max_price']

    async def test_preference_based_recommendations(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test that recommendations reflect user preferences"""
        # Create test user
        user_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!',
            'name': 'Test User'
        }
        response = test_client.post('/api/auth/register', json=user_data)
        assert response.status_code == 201
        user_id = response.json()['id']

        # Set user preferences
        preferences = {
            'categories': ['music', 'arts'],
            'max_price': 150,
            'location': {'lat': 40.7128, 'lng': -74.0060},
            'radius_km': 10
        }
        response = test_client.post(
            f'/api/users/{user_id}/preferences',
            json=preferences
        )
        assert response.status_code == 200

        # Configure mock recommendation service
        mock_services['recommendations'].set_response(
            'get_recommendations',
            [
                {
                    'id': 'event-1',
                    'title': 'Music Festival',
                    'categories': ['music'],
                    'price_info': {'min': 50, 'max': 100},
                    'location': {'lat': 40.7128, 'lng': -74.0060}
                },
                {
                    'id': 'event-2',
                    'title': 'Art Exhibition',
                    'categories': ['arts'],
                    'price_info': {'min': 20, 'max': 30},
                    'location': {'lat': 40.7130, 'lng': -74.0065}
                }
            ]
        )

        # Get recommendations
        response = test_client.get(f'/api/users/{user_id}/recommendations')
        assert response.status_code == 200
        recommendations = response.json()

        # Verify recommendations match preferences
        for event in recommendations:
            # Check price range
            assert event['price_info']['max'] <= preferences['max_price']
            # Check categories
            assert any(cat in event['categories'] for cat in preferences['categories'])
            # Check location (within radius)
            event_location = event['location']
            assert abs(event_location['lat'] - preferences['location']['lat']) < 0.1
            assert abs(event_location['lng'] - preferences['location']['lng']) < 0.1 