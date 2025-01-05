import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from app.utils.schema import Event, UserPreferences, Location, PriceInfo, SourceInfo
from app.models.preferences import UserPreferences as DBUserPreferences
from unittest.mock import patch, MagicMock

def test_get_recommendations_no_preferences(client, test_db, mock_supertokens):
    """Test getting recommendations when no preferences exist."""
    response = client.get("/api/recommendations")
    assert response.status_code == 200
    
    data = response.json()
    assert "recommendations" in data
    assert "events" in data["recommendations"]
    assert "pagination" in data
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["page_size"] == 20

def test_get_recommendations_with_preferences(client, test_db, mock_supertokens):
    """Test getting recommendations with user preferences."""
    # Create test preferences
    test_preferences = DBUserPreferences(
        user_id="test-user-123",
        preferred_categories=["Music", "Technology"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={"city": "san francisco"}
    )
    test_db.add(test_preferences)
    test_db.commit()
    
    response = client.get("/api/recommendations")
    assert response.status_code == 200
    
    data = response.json()
    assert "recommendations" in data
    assert "events" in data["recommendations"]
    assert "pagination" in data
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["page_size"] == 20

def test_get_recommendations_pagination(client, test_db, mock_supertokens):
    """Test recommendation pagination."""
    response = client.get("/api/recommendations?page=2&page_size=10")
    assert response.status_code == 200
    
    data = response.json()
    assert data["pagination"]["page"] == 2
    assert data["pagination"]["page_size"] == 10

def test_get_recommendations_date_range(client, test_db, mock_supertokens):
    """Test recommendation date range filtering."""
    response = client.get("/api/recommendations?days_ahead=7")
    assert response.status_code == 200
    
    data = response.json()
    for event in data["recommendations"]["events"]:
        event_date = datetime.fromisoformat(event["start_datetime"])
        assert event_date >= datetime.now()
        assert event_date <= datetime.now() + timedelta(days=7)

def test_get_recommendations_caching(client, test_db, mock_supertokens):
    """Test recommendation caching."""
    # First request should hit storage
    response1 = client.get("/api/recommendations")
    assert response1.status_code == 200
    
    # Second request should use cache
    response2 = client.get("/api/recommendations")
    assert response2.status_code == 200
    
    # Responses should be identical
    assert response1.json() == response2.json()

def test_get_recommendations_error_handling(client, test_db, mock_supertokens):
    """Test recommendation error handling."""
    # Test invalid page number
    response = client.get("/api/recommendations?page=0")
    assert response.status_code == 422
    
    # Test invalid page size
    response = client.get("/api/recommendations?page_size=0")
    assert response.status_code == 422
    response = client.get("/api/recommendations?page_size=101")
    assert response.status_code == 422
    
    # Test invalid days ahead
    response = client.get("/api/recommendations?days_ahead=0")
    assert response.status_code == 422
    response = client.get("/api/recommendations?days_ahead=366")
    assert response.status_code == 422 