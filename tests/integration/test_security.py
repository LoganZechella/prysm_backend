import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

@pytest.mark.asyncio
class TestSecurity:
    async def test_authentication_flow(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test complete authentication flow"""
        # Test user registration
        user_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!',
            'name': 'Test User'
        }
        response = test_client.post('/api/auth/register', json=user_data)
        assert response.status_code == 201
        user_id = response.json()['id']

        # Test login
        login_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!'
        }
        response = test_client.post('/api/auth/login', json=login_data)
        assert response.status_code == 200
        tokens = response.json()
        assert 'access_token' in tokens
        assert 'refresh_token' in tokens

        # Test authenticated request
        headers = {'Authorization': f'Bearer {tokens["access_token"]}'}
        response = test_client.get('/api/users/me', headers=headers)
        assert response.status_code == 200
        user_info = response.json()
        assert user_info['email'] == user_data['email']
        assert user_info['name'] == user_data['name']

        # Test token refresh
        headers = {'Authorization': f'Bearer {tokens["refresh_token"]}'}
        response = test_client.post('/api/auth/refresh', headers=headers)
        assert response.status_code == 200
        new_tokens = response.json()
        assert 'access_token' in new_tokens

        # Test logout
        headers = {'Authorization': f'Bearer {new_tokens["access_token"]}'}
        response = test_client.post('/api/auth/logout', headers=headers)
        assert response.status_code == 200

        # Verify token is invalidated
        response = test_client.get('/api/users/me', headers=headers)
        assert response.status_code == 401

    async def test_authorization_boundaries(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test authorization boundaries between users"""
        # Create two test users
        user1_data = {
            'email': 'user1@example.com',
            'password': 'TestPass123!',
            'name': 'User One'
        }
        response = test_client.post('/api/auth/register', json=user1_data)
        assert response.status_code == 201
        user1_id = response.json()['id']

        user2_data = {
            'email': 'user2@example.com',
            'password': 'TestPass123!',
            'name': 'User Two'
        }
        response = test_client.post('/api/auth/register', json=user2_data)
        assert response.status_code == 201
        user2_id = response.json()['id']

        # Login as user1
        response = test_client.post('/api/auth/login', json={
            'email': 'user1@example.com',
            'password': 'TestPass123!'
        })
        assert response.status_code == 200
        user1_token = response.json()['access_token']

        # Login as user2
        response = test_client.post('/api/auth/login', json={
            'email': 'user2@example.com',
            'password': 'TestPass123!'
        })
        assert response.status_code == 200
        user2_token = response.json()['access_token']

        # Set preferences for user1
        user1_prefs = {'categories': ['music', 'sports'], 'max_price': 100}
        headers = {'Authorization': f'Bearer {user1_token}'}
        response = test_client.post(f'/api/users/{user1_id}/preferences', json=user1_prefs, headers=headers)
        assert response.status_code == 200

        # Attempt to access user1's preferences with user2's token
        headers = {'Authorization': f'Bearer {user2_token}'}
        response = test_client.get(f'/api/users/{user1_id}/preferences', headers=headers)
        assert response.status_code == 403

    async def test_input_validation_security(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test input validation and security measures"""
        # Test SQL injection attempt
        login_data = {
            'email': "' OR '1'='1",
            'password': "' OR '1'='1"
        }
        response = test_client.post('/api/auth/login', json=login_data)
        assert response.status_code == 401

        # Test XSS attempt
        user_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!',
            'name': '<script>alert("XSS")</script>'
        }
        response = test_client.post('/api/auth/register', json=user_data)
        assert response.status_code == 422  # Validation error

        # Test invalid token format
        headers = {'Authorization': 'Bearer invalid_token_format'}
        response = test_client.get('/api/users/me', headers=headers)
        assert response.status_code == 401

        # Test expired token
        expired_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0QGV4YW1wbGUuY29tIiwiZXhwIjoxNTE2MjM5MDIyfQ.2lNYA8_M3S4Dz0BfgN2VtZ4ooZaBGvw4vEw7PZ-sbKQ'
        headers = {'Authorization': f'Bearer {expired_token}'}
        response = test_client.get('/api/users/me', headers=headers)
        assert response.status_code == 401

    async def test_rate_limiting(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test rate limiting functionality"""
        # Attempt rapid login requests
        login_data = {
            'email': 'test@example.com',
            'password': 'TestPass123!'
        }

        responses = []
        for _ in range(10):  # Attempt 10 rapid requests
            response = test_client.post('/api/auth/login', json=login_data)
            responses.append(response)

        # Verify rate limiting kicked in
        assert any(r.status_code == 429 for r in responses)  # Too Many Requests

        # Verify rate limit headers
        rate_limit_response = next(r for r in responses if r.status_code == 429)
        assert 'X-RateLimit-Limit' in rate_limit_response.headers
        assert 'X-RateLimit-Remaining' in rate_limit_response.headers
        assert 'X-RateLimit-Reset' in rate_limit_response.headers

    async def test_security_headers(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test security headers in responses"""
        # Test CORS headers for OPTIONS request
        response = test_client.options('/api/auth/login')
        assert response.status_code == 200
        headers = response.headers
        assert 'Access-Control-Allow-Origin' in headers
        assert 'Access-Control-Allow-Methods' in headers
        assert 'Access-Control-Allow-Headers' in headers

        # Test security headers in normal response
        response = test_client.get('/api/health')
        assert response.status_code == 200
        headers = response.headers
        assert headers.get('X-Content-Type-Options') == 'nosniff'
        assert headers.get('X-Frame-Options') == 'DENY'
        assert headers.get('X-XSS-Protection') == '1; mode=block'
        assert 'Content-Security-Policy' in headers
        assert 'Strict-Transport-Security' in headers 