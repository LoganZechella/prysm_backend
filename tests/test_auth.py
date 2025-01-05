import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.database import get_db, Base
from tests.conftest import test_db, client, mock_session, mock_supertokens
from unittest.mock import MagicMock

def test_signup_success(client, mock_supertokens):
    """Test successful user sign up."""
    signup_data = {
        "email": "test@example.com",
        "password": "StrongPass123!",
        "name": "Test User"
    }
    
    response = client.post("/api/auth/signup", json=signup_data)
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "success"
    assert data["message"] == "User signed up successfully"
    assert data["user"]["email"] == signup_data["email"]
    assert data["user"]["name"] == signup_data["name"]
    assert data["user"]["id"] == "test-user-123"
    
    # Verify SuperTokens was called correctly
    mock_supertokens["sign_up"].assert_called_once_with(
        tenant_id="public",
        email=signup_data["email"],
        password=signup_data["password"],
        user_context={"name": signup_data["name"]}
    )

def test_signup_invalid_email(client, mock_supertokens):
    """Test sign up with invalid email."""
    signup_data = {
        "email": "invalid-email",
        "password": "StrongPass123!",
        "name": "Test User"
    }
    
    response = client.post("/api/auth/signup", json=signup_data)
    assert response.status_code == 422  # Validation error
    
    # Verify SuperTokens was not called
    mock_supertokens["sign_up"].assert_not_called()

def test_signup_weak_password(client, mock_supertokens):
    """Test sign up with weak password."""
    # Mock sign up failure
    mock_supertokens["sign_up"].return_value = MagicMock(user=None)
    
    signup_data = {
        "email": "test@example.com",
        "password": "weak",
        "name": "Test User"
    }
    
    response = client.post("/api/auth/signup", json=signup_data)
    assert response.status_code == 400  # Bad request

def test_signin_success(client, mock_supertokens):
    """Test successful user sign in."""
    signin_data = {
        "email": "test@example.com",
        "password": "StrongPass123!"
    }
    
    response = client.post("/api/auth/signin", json=signin_data)
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "success"
    assert data["message"] == "User signed in successfully"
    assert data["user"]["email"] == signin_data["email"]
    assert data["user"]["id"] == "test-user-123"
    
    # Verify SuperTokens was called correctly
    mock_supertokens["sign_in"].assert_called_once_with(
        tenant_id="public",
        email=signin_data["email"],
        password=signin_data["password"]
    )

def test_signin_wrong_password(client, mock_supertokens):
    """Test sign in with wrong password."""
    # Mock sign in failure
    mock_supertokens["sign_in"].return_value = MagicMock(user=None)
    
    signin_data = {
        "email": "test@example.com",
        "password": "WrongPass123!"
    }
    
    response = client.post("/api/auth/signin", json=signin_data)
    assert response.status_code == 401  # Unauthorized

def test_signin_nonexistent_user(client, mock_supertokens):
    """Test sign in with nonexistent user."""
    # Mock sign in failure
    mock_supertokens["sign_in"].return_value = MagicMock(user=None)
    
    signin_data = {
        "email": "nonexistent@example.com",
        "password": "StrongPass123!"
    }
    
    response = client.post("/api/auth/signin", json=signin_data)
    assert response.status_code == 401  # Unauthorized 