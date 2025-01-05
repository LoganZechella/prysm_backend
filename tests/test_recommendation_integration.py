import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
import json
from app.main import app
from app.utils.schema import Event, Location, PriceInfo, EventAttributes, SourceInfo, EventMetadata
from app.utils.storage import StorageManager
from app.models.preferences import UserPreferences as DBUserPreferences
from unittest.mock import patch, MagicMock

pytestmark = pytest.mark.asyncio

@pytest.fixture
def test_client():
    return TestClient(app)

@pytest.fixture
def mock_storage():
    """Mock storage manager to return test events"""
    with patch('app.routes.recommendations.storage_manager') as mock:
        # Create test events
        events = [
            Event(
                event_id=f"test-event-{i}",
                title=f"Test Event {i}",
                description=f"Description for test event {i}",
                start_datetime=datetime.now() + timedelta(days=i),
                end_datetime=datetime.now() + timedelta(days=i, hours=2),  # Event duration of 2 hours
                location=Location(
                    venue_name=f"Venue {i}",
                    city="San Francisco",
                    coordinates={"lat": 37.7749, "lng": -122.4194}
                ),
                categories=["Music", "Technology"] if i % 2 == 0 else ["Sports"],
                price_info=PriceInfo(
                    min_price=50.0 * i,
                    max_price=100.0 * i,
                    price_tier="medium" if i < 3 else "premium"
                ),
                attributes=EventAttributes(
                    indoor_outdoor="indoor",
                    age_restriction="all",
                    accessibility_features=["wheelchair"]
                ),
                source=SourceInfo(
                    platform="test",
                    url=f"http://test.com/event{i}",
                    last_updated=datetime.now()
                ),
                metadata=EventMetadata(
                    popularity_score=0.8 - (i * 0.1)
                )
            )
            for i in range(5)
        ]
        
        # Mock storage methods
        async def mock_list_events(*args, **kwargs):
            return [f"event_{i}.json" for i in range(5)]
            
        async def mock_get_event(*args, **kwargs):
            event_idx = int(args[0].split('_')[1].split('.')[0])
            return events[event_idx].model_dump()
            
        mock.list_processed_events.side_effect = mock_list_events
        mock.get_processed_event.side_effect = mock_get_event
        yield mock

@pytest.fixture
def mock_auth():
    """Mock authentication to return test user"""
    with patch('app.routes.recommendations.verify_session') as mock:
        session = MagicMock()
        session.get_user_id.return_value = "test-user-123"
        mock.return_value = session
        yield mock

@pytest.fixture
def test_preferences(test_db):
    """Create test user preferences in the database"""
    preferences = DBUserPreferences(
        user_id="test-user-123",
        preferred_categories=["Music", "Technology"],
        min_price=0.0,
        max_price=300.0,
        preferred_location={"city": "san francisco"},
        preferred_days=[],
        accessibility_requirements=[],
        indoor_outdoor_preference=None,
        age_restriction_preference=None
    )
    test_db.add(preferences)
    test_db.commit()
    return preferences

@pytest.mark.asyncio
async def test_get_recommendations_integration(
    test_client,
    mock_storage,
    mock_auth,
    test_preferences,
    test_db
):
    """Test the complete recommendation workflow"""
    response = test_client.get("/api/recommendations?page=1&page_size=10&days_ahead=30")
    assert response.status_code == 200
    
    data = response.json()
    assert "recommendations" in data
    assert "events" in data["recommendations"]
    assert len(data["recommendations"]["events"]) > 0
    
    # Verify recommendation structure
    first_rec = data["recommendations"]["events"][0]
    assert all(key in first_rec for key in [
        "event_id", "title", "description", "start_datetime",
        "location", "categories", "price_info", "match_score"
    ])
    
    # Verify recommendations are ordered by relevance
    scores = [rec["match_score"] for rec in data["recommendations"]["events"]]
    assert scores == sorted(scores, reverse=True)
    
    # Verify pagination
    assert "pagination" in data
    assert data["pagination"]["page"] == 1
    assert data["pagination"]["page_size"] == 10
    
    # Test with different page size
    response = test_client.get("/api/recommendations?page=1&page_size=2&days_ahead=30")
    assert response.status_code == 200
    data = response.json()
    assert len(data["recommendations"]["events"]) <= 2
    
    # Test with invalid parameters
    response = test_client.get("/api/recommendations?page=0&page_size=10&days_ahead=30")
    assert response.status_code == 422
    
    response = test_client.get("/api/recommendations?page=1&page_size=0&days_ahead=30")
    assert response.status_code == 422
    
    response = test_client.get("/api/recommendations?page=1&page_size=10&days_ahead=0")
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_recommendation_caching(
    test_client,
    mock_storage,
    mock_auth,
    test_preferences,
    test_db
):
    """Test that recommendations are properly cached"""
    # First request should hit storage
    response1 = test_client.get("/api/recommendations?page=1&page_size=10&days_ahead=30")
    assert response1.status_code == 200
    
    # Second request should use cache
    response2 = test_client.get("/api/recommendations?page=1&page_size=10&days_ahead=30")
    assert response2.status_code == 200
    
    # Compare responses ignoring timestamps
    data1 = response1.json()
    data2 = response2.json()
    
    # Remove timestamps before comparison
    data1.pop("timestamp")
    data2.pop("timestamp")
    
    assert data1 == data2
    
    # Verify storage was only called once
    list_calls = mock_storage.list_processed_events.call_count
    get_calls = mock_storage.get_processed_event.call_count
    assert list_calls == 2  # One call per month
    assert get_calls == 10  # Five events per month

@pytest.mark.asyncio
async def test_recommendations_with_preferences(
    test_client,
    mock_storage,
    mock_auth,
    test_db
):
    """Test recommendations with different user preferences"""
    # Create preferences favoring expensive events
    preferences = DBUserPreferences(
        user_id="test-user-123",
        preferred_categories=["Sports"],
        min_price=200.0,
        max_price=1000.0,
        preferred_location={"city": "san francisco"},
        preferred_days=[],
        accessibility_requirements=[],
        indoor_outdoor_preference=None,
        age_restriction_preference=None
    )
    test_db.add(preferences)
    test_db.commit()
    test_db.refresh(preferences)  # Ensure preferences are loaded
    
    # Clear any existing cache
    from app.routes.recommendations import recommendations_cache, events_cache
    recommendations_cache.clear()
    events_cache.clear()
    
    response = test_client.get("/api/recommendations?page=1&page_size=10&days_ahead=30")
    assert response.status_code == 200
    
    data = response.json()
    events = data["recommendations"]["events"]
    
    # Verify recommendations match preferences
    for event in events:
        assert event["price_info"]["min_price"] >= 200.0 or event["price_info"]["max_price"] >= 200.0
        assert "Sports" in event["categories"] 