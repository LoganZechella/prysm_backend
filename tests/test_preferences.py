import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.database import get_db
from app.models.preferences import UserPreferences
from unittest.mock import patch

def test_get_preferences_no_preferences(client, mock_supertokens):
    """Test getting preferences when none exist."""
    response = client.get("/api/preferences/")
    assert response.status_code == 200
    
    data = response.json()
    assert data["preferred_categories"] == []
    assert data["excluded_categories"] == []
    assert data["min_price"] == 0
    assert data["max_price"] == 1000
    assert data["preferred_location"] == {
        "city": "",
        "state": "",
        "country": "",
        "max_distance_km": 50
    }
    assert data["preferred_days"] == []
    assert data["preferred_times"] == []
    assert data["min_rating"] == 0.5

def test_update_preferences(client, mock_supertokens, test_db):
    """Test updating preferences."""
    preferences = {
        "preferred_categories": ["restaurants", "bars"],
        "excluded_categories": ["clubs"],
        "min_price": 10,
        "max_price": 100,
        "preferred_location": {
            "city": "San Francisco",
            "state": "CA",
            "country": "USA",
            "max_distance_km": 25
        },
        "preferred_days": ["friday", "saturday"],
        "preferred_times": ["evening"],
        "min_rating": 4.0
    }
    
    response = client.post("/api/preferences/", json=preferences)
    assert response.status_code == 200
    assert response.json() == {"status": "success", "message": "Preferences updated"}
    
    # Verify preferences were saved
    user_preferences = test_db.query(UserPreferences).filter(
        UserPreferences.user_id == "test-user-123"
    ).first()
    
    assert user_preferences is not None
    assert user_preferences.preferred_categories == preferences["preferred_categories"]
    assert user_preferences.excluded_categories == preferences["excluded_categories"]
    assert user_preferences.min_price == preferences["min_price"]
    assert user_preferences.max_price == preferences["max_price"]
    assert user_preferences.preferred_location == preferences["preferred_location"]
    assert user_preferences.preferred_days == preferences["preferred_days"]
    assert user_preferences.preferred_times == preferences["preferred_times"]
    assert user_preferences.min_rating == preferences["min_rating"]

def test_update_preferences_invalid_data(client, mock_supertokens):
    """Test updating preferences with invalid data."""
    invalid_preferences = {
        "preferred_categories": ["restaurants", "bars"],
        "excluded_categories": ["clubs"],
        "min_price": "invalid",  # Should be a number
        "max_price": 100,
        "preferred_location": {
            "city": "San Francisco",
            "state": "CA",
            "country": "USA",
            "max_distance_km": 25
        },
        "preferred_days": ["friday", "saturday"],
        "preferred_times": ["evening"],
        "min_rating": 4.0
    }
    
    response = client.post("/api/preferences/", json=invalid_preferences)
    assert response.status_code == 422  # Validation error 