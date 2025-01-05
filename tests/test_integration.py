import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from app.utils.schema import Event, UserPreferences, Location, PriceInfo, SourceInfo
from app.models.preferences import UserPreferences as DBUserPreferences
import json

def test_get_recommendations_pagination(client, test_db, mock_supertokens, test_events, monkeypatch):
    """Test recommendation endpoint with pagination."""
    # Mock storage manager to return test events
    async def mock_get_events(*args, **kwargs):
        return test_events
    
    from app.routes.recommendations import get_available_events
    monkeypatch.setattr("app.routes.recommendations.get_available_events", mock_get_events)
    
    # Create test user preferences
    test_preferences = DBUserPreferences(
        user_id="test-user-123",
        preferred_categories=["Music", "Technology"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={"city": "san francisco"}
    )
    test_db.add(test_preferences)
    test_db.commit()
    
    # Test different page sizes
    page_sizes = [5, 10, 20]
    for page_size in page_sizes:
        response = client.get(f"/api/recommendations?page=1&page_size={page_size}")
        assert response.status_code == 200
        data = response.json()
        
        # Check pagination metadata
        assert "pagination" in data
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == page_size
        assert data["pagination"]["total_items"] > 0
        
        # Check number of events returned matches page size
        events = data["recommendations"]["events"]
        assert len(events) <= page_size
        
        # Verify event structure
        for event in events:
            assert "event_id" in event
            assert "title" in event
            assert "match_score" in event
            assert "location" in event
            assert "coordinates" in event["location"]

def test_get_recommendations_cache(client, test_db, mock_supertokens, test_events, monkeypatch):
    """Test recommendation endpoint caching."""
    # Mock storage manager to count calls
    call_count = 0
    async def mock_get_events(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return test_events
    
    from app.routes.recommendations import get_available_events
    monkeypatch.setattr("app.routes.recommendations.get_available_events", mock_get_events)
    
    # Create test user preferences
    test_preferences = DBUserPreferences(
        user_id="test-user-123",
        preferred_categories=["Music", "Technology"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={"city": "san francisco"}
    )
    test_db.add(test_preferences)
    test_db.commit()
    
    # First request should hit storage
    response1 = client.get("/api/recommendations?page=1&page_size=10")
    assert response1.status_code == 200
    assert call_count == 1
    
    # Second request should use cache
    response2 = client.get("/api/recommendations?page=1&page_size=10")
    assert response2.status_code == 200
    assert call_count == 1  # Count shouldn't increase
    
    # Different page should still use cached events
    response3 = client.get("/api/recommendations?page=2&page_size=10")
    assert response3.status_code == 200
    assert call_count == 1
    
    # Verify different pages return different events
    events1 = response1.json()["recommendations"]["events"]
    events2 = response2.json()["recommendations"]["events"]
    events3 = response3.json()["recommendations"]["events"]
    
    assert events1 == events2  # Same page should return same events
    assert events1 != events3  # Different pages should return different events

def test_get_recommendations_date_range(client, test_db, mock_supertokens, test_events, monkeypatch):
    """Test recommendation endpoint with different date ranges."""
    # Mock storage manager to return test events
    async def mock_get_events(*args, **kwargs):
        return test_events
    
    from app.routes.recommendations import get_available_events
    monkeypatch.setattr("app.routes.recommendations.get_available_events", mock_get_events)
    
    # Create test user preferences
    test_preferences = DBUserPreferences(
        user_id="test-user-123",
        preferred_categories=["Music", "Technology"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={"city": "san francisco"}
    )
    test_db.add(test_preferences)
    test_db.commit()
    
    # Test different date ranges
    days_ahead_values = [7, 30, 90]
    for days_ahead in days_ahead_values:
        response = client.get(f"/api/recommendations?days_ahead={days_ahead}")
        assert response.status_code == 200
        data = response.json()
        
        # Check events are within date range
        events = data["recommendations"]["events"]
        for event in events:
            event_date = datetime.fromisoformat(event["start_datetime"])
            assert event_date >= datetime.now()
            assert event_date <= datetime.now() + timedelta(days=days_ahead)

def test_get_recommendations_error_handling(client, test_db, mock_supertokens):
    """Test recommendation endpoint error handling."""
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

def test_get_recommendations_empty_results(client, test_db, mock_supertokens, monkeypatch):
    """Test recommendation endpoint with no available events."""
    # Mock storage manager to return no events
    async def mock_get_events(*args, **kwargs):
        return []
    
    from app.routes.recommendations import get_available_events
    monkeypatch.setattr("app.routes.recommendations.get_available_events", mock_get_events)
    
    # Create test user preferences
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
    
    # Check empty response structure
    assert len(data["recommendations"]["events"]) == 0
    assert data["pagination"]["total_items"] == 0
    assert data["pagination"]["total_pages"] == 0 