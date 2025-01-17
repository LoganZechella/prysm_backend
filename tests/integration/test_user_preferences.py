import pytest
from datetime import datetime
from typing import Dict, Any, List

class TestUserPreferences:
    async def test_preference_update_propagation(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test user preference changes propagate correctly"""
        # Setup initial preferences
        user_id = 'user-1'
        initial_prefs = {
            'categories': ['music', 'sports'],
            'price_range': {'min': 0, 'max': 100},
            'location_preference': {
                'city': 'New York',
                'coordinates': {'lat': 40.7128, 'lng': -74.0060},
                'radius_km': 50
            }
        }
        
        # Set initial preferences
        response = await test_client.post(
            f'/api/users/{user_id}/preferences',
            json=initial_prefs
        )
        assert response.status_code == 200
        
        # Update preferences
        updated_prefs = {
            'categories': ['music', 'arts'],
            'price_range': {'min': 50, 'max': 200},
            'location_preference': {
                'city': 'Brooklyn',
                'coordinates': {'lat': 40.6782, 'lng': -73.9442},
                'radius_km': 30
            }
        }
        
        response = await test_client.put(
            f'/api/users/{user_id}/preferences',
            json=updated_prefs
        )
        assert response.status_code == 200
        
        # Verify preferences were updated in database
        async with test_db._pool.acquire() as conn:
            stored_prefs = await conn.fetchrow(
                'SELECT * FROM preferences WHERE user_id = $1',
                user_id
            )
            assert stored_prefs['categories'] == updated_prefs['categories']
            assert stored_prefs['price_range'] == updated_prefs['price_range']
            
            # Verify recommendation cache was invalidated
            cache_entry = await conn.fetchrow(
                'SELECT * FROM recommendation_cache WHERE user_id = $1',
                user_id
            )
            assert cache_entry is None
            
            # Verify preference history was recorded
            pref_history = await conn.fetch(
                'SELECT * FROM preference_history WHERE user_id = $1 ORDER BY timestamp DESC',
                user_id
            )
            assert len(pref_history) == 2

    async def test_concurrent_preference_updates(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test handling of concurrent preference updates"""
        user_id = 'user-2'
        
        # Create multiple concurrent update requests
        async def update_preferences(prefs):
            return await test_client.put(
                f'/api/users/{user_id}/preferences',
                json=prefs
            )
        
        import asyncio
        update_requests = [
            {'categories': ['music'], 'price_range': {'min': 0, 'max': 100}},
            {'categories': ['sports'], 'price_range': {'min': 50, 'max': 200}},
            {'categories': ['arts'], 'price_range': {'min': 25, 'max': 150}}
        ]
        
        responses = await asyncio.gather(
            *[update_preferences(prefs) for prefs in update_requests]
        )
        
        # Verify all requests were processed
        assert all(r.status_code == 200 for r in responses)
        
        # Verify final state is consistent
        async with test_db._pool.acquire() as conn:
            final_prefs = await conn.fetchrow(
                'SELECT * FROM preferences WHERE user_id = $1',
                user_id
            )
            assert len(final_prefs['categories']) == 1  # Only one category should remain
            assert final_prefs['version'] == 3  # Three updates

    async def test_preference_based_recommendations(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test recommendations reflect user preferences"""
        user_id = 'user-1'
        
        # Set user preferences
        preferences = {
            'categories': ['music', 'festival'],
            'price_range': {'min': 0, 'max': 200},
            'location_preference': {
                'city': 'New York',
                'coordinates': {'lat': 40.7128, 'lng': -74.0060},
                'radius_km': 50
            }
        }
        
        response = await test_client.put(
            f'/api/users/{user_id}/preferences',
            json=preferences
        )
        assert response.status_code == 200
        
        # Configure mock recommendation service
        mock_events = [
            {
                'id': 'event-1',
                'title': 'Rock Concert',
                'categories': ['music'],
                'price': 150
            },
            {
                'id': 'event-2',
                'title': 'Classical Concert',
                'categories': ['music'],
                'price': 300  # Outside price range
            },
            {
                'id': 'event-3',
                'title': 'Food Festival',
                'categories': ['food', 'festival'],
                'price': 50
            }
        ]
        mock_services['recommendations'].set_response(
            f'get_recommendations_{user_id}',
            mock_events
        )
        
        # Get recommendations
        response = await test_client.get(f'/api/users/{user_id}/recommendations')
        assert response.status_code == 200
        recommendations = response.json()
        
        # Verify recommendations match preferences
        assert len(recommendations) == 2  # Only events within price range
        assert all(
            any(cat in preferences['categories'] for cat in event['categories'])
            for event in recommendations
        )
        assert all(
            preferences['price_range']['min'] <= event['price'] <= preferences['price_range']['max']
            for event in recommendations
        ) 