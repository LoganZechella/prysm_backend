import pytest
from datetime import datetime, timedelta
from app.utils.schema import Event, Location, PriceInfo, EventAttributes, SourceInfo, EventMetadata
from app.utils.data_quality import DataQualityService, DataQualityMetrics, DataProfile

@pytest.fixture
def sample_events():
    """Create sample events for testing"""
    base_time = datetime.now() + timedelta(days=1)
    events = []
    
    # Valid event
    events.append(Event(
        event_id="event1",
        title="Test Event 1",
        description="This is a test event with proper description that meets the minimum length requirement.",
        start_datetime=base_time,
        end_datetime=base_time + timedelta(hours=2),
        location=Location(
            venue_name="Test Venue",
            city="Test City",
            coordinates={"lat": 37.7749, "lng": -122.4194}
        ),
        categories=["music", "concert"],
        price_info=PriceInfo(min_price=50.0, max_price=100.0),
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=datetime.now()
        ),
        metadata=EventMetadata(popularity_score=0.8)
    ))
    
    # Event with validation issues
    events.append(Event(
        event_id="event2",
        title="123 Invalid Title",
        description="Too short",
        start_datetime=base_time - timedelta(days=2),  # Past event
        location=Location(
            venue_name="",  # Empty venue
            city="Test City",
            coordinates={"lat": 100.0, "lng": -200.0}  # Invalid coordinates
        ),
        categories=[],  # Empty categories
        price_info=PriceInfo(min_price=100.0, max_price=50.0),  # Invalid price range
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=datetime.now()
        ),
        metadata=EventMetadata(popularity_score=0.5)
    ))
    
    # Event with anomalies
    events.append(Event(
        event_id="event3",
        title="Anomalous Event",
        description="This event has unusual characteristics that should be detected as anomalies.",
        start_datetime=base_time,
        end_datetime=base_time + timedelta(days=10),  # Unusually long duration
        location=Location(
            venue_name="Test Venue",
            city="Test City",
            coordinates={"lat": 37.7749, "lng": -122.4194}
        ),
        categories=["cat1", "cat2", "cat3", "cat4", "cat5", "cat6", "cat7", "cat8", "cat9", "cat10", "cat11"],
        price_info=PriceInfo(min_price=1000.0, max_price=5000.0),  # Unusually high price
        source=SourceInfo(
            platform="test",
            url="http://test.com",
            last_updated=datetime.now()
        ),
        metadata=EventMetadata(popularity_score=0.9)
    ))
    
    return events

def test_validate_event(sample_events):
    """Test event validation"""
    service = DataQualityService()
    
    # Test valid event
    is_valid, errors = service.validate_event(sample_events[0])
    assert is_valid
    assert not errors
    
    # Test invalid event
    is_valid, errors = service.validate_event(sample_events[1])
    assert not is_valid
    assert len(errors) >= 5  # Should have multiple validation errors
    assert any("Title" in error for error in errors)
    assert any("venue" in error for error in errors)
    assert any("coordinates" in error for error in errors)
    assert any("categories" in error for error in errors)
    assert any("price" in error for error in errors)

def test_profile_events(sample_events):
    """Test event data profiling"""
    service = DataQualityService()
    profile = service.profile_events(sample_events)
    
    # Check field types
    assert "title" in profile.field_types
    assert "description" in profile.field_types
    
    # Check unique values
    assert profile.unique_values["title"] == 3
    
    # Check value distributions
    assert "categories" in profile.value_distributions
    
    # Check numeric stats
    assert "popularity_score" in profile.numeric_stats
    stats = profile.numeric_stats["popularity_score"]
    assert all(key in stats for key in ["mean", "std", "min", "max", "median"])

def test_calculate_completeness(sample_events):
    """Test completeness calculation"""
    service = DataQualityService()
    completeness = service.calculate_completeness(sample_events)
    
    # All required fields should be present
    assert "title" in completeness
    assert "description" in completeness
    assert "start_datetime" in completeness
    
    # Check completeness scores
    assert all(0 <= score <= 100 for score in completeness.values())
    assert completeness["title"] == 100  # All events have titles

def test_detect_anomalies(sample_events):
    """Test anomaly detection"""
    service = DataQualityService()
    anomalies = service.detect_anomalies(sample_events)
    
    # Check anomalous event
    assert "event3" in anomalies
    event3_anomalies = anomalies["event3"]
    assert any("price" in anomaly.lower() for anomaly in event3_anomalies)
    assert any("duration" in anomaly.lower() for anomaly in event3_anomalies)
    assert any("categories" in anomaly.lower() for anomaly in event3_anomalies)
    
    # Check normal event
    assert "event1" not in anomalies

def test_generate_quality_report(sample_events):
    """Test quality report generation"""
    service = DataQualityService()
    report = service.generate_quality_report(sample_events)
    
    # Check report structure
    assert "metrics" in report
    assert "profile" in report
    assert "completeness" in report
    assert "anomalies" in report
    assert "quality_score" in report
    assert "timestamp" in report
    
    # Check quality scores
    quality_score = report["quality_score"]
    assert all(0 <= score <= 100 for score in quality_score.values())
    assert "validity" in quality_score
    assert "completeness" in quality_score
    assert "anomaly_free" in quality_score
    assert "overall" in quality_score 