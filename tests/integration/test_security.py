import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List

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
            'email': 'test.user@example.com',
            'password': 'SecurePass123!',
            'name': 'Test User'
        }
        
        response = await test_client.post('/api/auth/register', json=user_data)
        assert response.status_code == 200
        
        # Test login
        login_data = {
            'email': user_data['email'],
            'password': user_data['password']
        }
        response = await test_client.post('/api/auth/login', json=login_data)
        assert response.status_code == 200
        token = response.json()['token']
        
        # Test authenticated request
        headers = {'Authorization': f'Bearer {token}'}
        response = await test_client.get('/api/users/me', headers=headers)
        assert response.status_code == 200
        assert response.json()['email'] == user_data['email']
        
        # Test token refresh
        response = await test_client.post(
            '/api/auth/refresh',
            headers=headers
        )
        assert response.status_code == 200
        new_token = response.json()['token']
        assert new_token != token
        
        # Test logout
        response = await test_client.post(
            '/api/auth/logout',
            headers={'Authorization': f'Bearer {new_token}'}
        )
        assert response.status_code == 200
        
        # Verify token is invalidated
        response = await test_client.get(
            '/api/users/me',
            headers={'Authorization': f'Bearer {new_token}'}
        )
        assert response.status_code == 401

    async def test_authorization_boundaries(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test authorization boundaries and access control"""
        # Create two test users
        users = [
            {
                'email': f'user{i}@example.com',
                'password': 'SecurePass123!',
                'name': f'User {i}'
            }
            for i in range(2)
        ]
        
        tokens = []
        for user in users:
            # Register
            await test_client.post('/api/auth/register', json=user)
            # Login
            response = await test_client.post('/api/auth/login', json={
                'email': user['email'],
                'password': user['password']
            })
            tokens.append(response.json()['token'])
        
        # Set preferences for first user
        preferences = {
            'categories': ['music'],
            'price_range': {'min': 0, 'max': 100}
        }
        response = await test_client.post(
            '/api/users/preferences',
            headers={'Authorization': f'Bearer {tokens[0]}'},
            json=preferences
        )
        assert response.status_code == 200
        
        # Attempt to access first user's preferences with second user's token
        response = await test_client.get(
            '/api/users/preferences',
            headers={'Authorization': f'Bearer {tokens[1]}'}
        )
        assert response.status_code == 403

    async def test_input_validation_security(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test input validation and security measures"""
        # Test SQL injection attempt
        malicious_input = {
            'email': "' OR '1'='1",
            'password': "' OR '1'='1"
        }
        response = await test_client.post('/api/auth/login', json=malicious_input)
        assert response.status_code == 400
        
        # Test XSS attempt
        malicious_event = {
            'title': '<script>alert("xss")</script>Event',
            'description': 'Normal description'
        }
        response = await test_client.post('/api/events', json=malicious_event)
        assert response.status_code == 400
        
        # Test invalid token format
        response = await test_client.get(
            '/api/users/me',
            headers={'Authorization': 'Bearer invalid_token_format'}
        )
        assert response.status_code == 401
        
        # Test expired token
        expired_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyQGV4YW1wbGUuY29tIiwiZXhwIjoxNTE2MjM5MDIyfQ.2lNYA8_jg5WE8iWV-04K_1cQfw8zcX8zQLGJX0vqKdA'
        response = await test_client.get(
            '/api/users/me',
            headers={'Authorization': f'Bearer {expired_token}'}
        )
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
            'password': 'password123'
        }
        
        responses = []
        for _ in range(10):  # Attempt 10 rapid requests
            response = await test_client.post(
                '/api/auth/login',
                json=login_data
            )
            responses.append(response)
        
        # Verify rate limiting kicked in
        assert any(r.status_code == 429 for r in responses)
        
        # Test rate limiting for API endpoints
        headers = {'Authorization': 'Bearer valid_token'}
        responses = []
        for _ in range(100):  # Attempt 100 rapid requests
            response = await test_client.get(
                '/api/events',
                headers=headers
            )
            responses.append(response)
        
        assert any(r.status_code == 429 for r in responses)

    async def test_security_headers(
        self,
        test_db,
        mock_services,
        test_client
    ):
        """Test security headers and configurations"""
        response = await test_client.get('/')
        headers = response.headers
        
        # Verify security headers are present
        assert headers.get('X-Content-Type-Options') == 'nosniff'
        assert headers.get('X-Frame-Options') == 'DENY'
        assert headers.get('X-XSS-Protection') == '1; mode=block'
        assert 'Content-Security-Policy' in headers
        
        # Verify CORS headers for OPTIONS request
        response = await test_client.options('/')
        headers = response.headers
        assert 'Access-Control-Allow-Origin' in headers
        assert 'Access-Control-Allow-Methods' in headers
        assert 'Access-Control-Allow-Headers' in headers 