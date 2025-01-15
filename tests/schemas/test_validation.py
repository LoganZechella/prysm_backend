"""
Tests for data validation schemas and pipeline.
"""

import pytest
from datetime import datetime, timedelta
from app.schemas.validation import (
    SchemaVersion,
    Category,
    Location,
    Price,
    EventV1,
    EventV2,
    ValidationPipeline
)

@pytest.fixture
def valid_category():
    return {
        "name": "Music Festival",
        "parent": "Music",
        "confidence": 0.95
    }

@pytest.fixture
def valid_location():
    return {
        "name": "Central Park",
        "address": "Central Park West",
        "city": "New York",
        "state": "NY",
        "country": "USA",
        "postal_code": "10024",
        "latitude": 40.7829,
        "longitude": -73.9654
    }

@pytest.fixture
def valid_price():
    return {
        "amount": 49.99,
        "currency": "USD",
        "tier": "General Admission"
    }

@pytest.fixture
def valid_event_base():
    return {
        "title": "Summer Music Festival 2024",
        "description": "Annual summer music festival featuring top artists",
        "start_date": datetime.now() + timedelta(days=30),
        "end_date": datetime.now() + timedelta(days=32),
        "categories": [{"name": "Music Festival", "confidence": 0.95}],
        "location": {
            "name": "Central Park",
            "address": "Central Park West",
            "city": "New York",
            "state": "NY",
            "country": "USA",
            "postal_code": "10024"
        },
        "prices": [{
            "amount": 49.99,
            "currency": "USD",
            "tier": "General Admission"
        }],
        "source": "eventbrite",
        "external_id": "evt-123456",
        "metadata": {"featured_artists": ["Artist 1", "Artist 2"]}
    }

def test_category_validation():
    # Test valid category
    category = Category(**valid_category())
    assert category.name == "Music Festival"
    assert category.confidence == 0.95
    
    # Test invalid category name
    with pytest.raises(ValueError, match="Category name contains invalid characters"):
        Category(name="Invalid@Category", confidence=0.9)
    
    # Test invalid confidence
    with pytest.raises(ValueError):
        Category(name="Valid Category", confidence=1.5)

def test_location_validation():
    # Test valid location
    location = Location(**valid_location())
    assert location.name == "Central Park"
    assert location.postal_code == "10024"
    
    # Test invalid postal code
    with pytest.raises(ValueError, match="Invalid postal code format"):
        Location(**{**valid_location(), "postal_code": "invalid@code"})
    
    # Test invalid coordinates
    with pytest.raises(ValueError):
        Location(**{**valid_location(), "latitude": 91.0})
    with pytest.raises(ValueError):
        Location(**{**valid_location(), "longitude": -181.0})

def test_price_validation():
    # Test valid price
    price = Price(**valid_price())
    assert price.amount == 49.99
    assert price.currency == "USD"
    
    # Test invalid amount
    with pytest.raises(ValueError):
        Price(**{**valid_price(), "amount": -10.0})
    
    # Test invalid currency
    with pytest.raises(ValueError, match="Currency must be a 3-letter ISO code"):
        Price(**{**valid_price(), "currency": "INVALID"})

def test_event_v1_validation(valid_event_base):
    # Test valid event
    event = EventV1(**valid_event_base)
    assert event.title == "Summer Music Festival 2024"
    assert event.schema_version == SchemaVersion.V1
    
    # Test invalid title
    with pytest.raises(ValueError, match="Title cannot be empty"):
        EventV1(**{**valid_event_base, "title": ""})
    
    # Test invalid dates
    invalid_dates = {
        **valid_event_base,
        "start_date": datetime.now() + timedelta(days=2),
        "end_date": datetime.now() + timedelta(days=1)
    }
    with pytest.raises(ValueError, match="End date must be after start date"):
        EventV1(**invalid_dates)

def test_event_v2_validation(valid_event_base):
    # Add V2 specific fields
    v2_data = {
        **valid_event_base,
        "status": "active",
        "featured": True,
        "tags": ["music", "festival", "summer"],
        "attendance_mode": "in_person",
        "capacity": 5000,
        "organizer": {"name": "Event Corp", "contact": "contact@example.com"}
    }
    
    # Test valid event
    event = EventV2(**v2_data)
    assert event.schema_version == SchemaVersion.V2
    assert event.status == "active"
    assert len(event.tags) == 3
    
    # Test invalid status
    with pytest.raises(ValueError, match="Invalid status"):
        EventV2(**{**v2_data, "status": "invalid"})
    
    # Test invalid attendance mode
    with pytest.raises(ValueError, match="Invalid attendance mode"):
        EventV2(**{**v2_data, "attendance_mode": "invalid"})
    
    # Test invalid capacity
    with pytest.raises(ValueError):
        EventV2(**{**v2_data, "capacity": -1})

def test_validation_pipeline(valid_event_base):
    pipeline = ValidationPipeline()
    
    # Test V1 validation
    v1_result = pipeline.validate_event(valid_event_base, version=SchemaVersion.V1)
    assert v1_result['schema_version'] == SchemaVersion.V1
    
    # Test V2 validation
    v2_data = {
        **valid_event_base,
        "status": "active",
        "attendance_mode": "in_person"
    }
    v2_result = pipeline.validate_event(v2_data, version=SchemaVersion.V2)
    assert v2_result['schema_version'] == SchemaVersion.V2
    
    # Test schema upgrade
    upgraded = pipeline.upgrade_schema(valid_event_base)
    assert upgraded['schema_version'] == SchemaVersion.V2
    assert upgraded['status'] == "active"
    assert upgraded['attendance_mode'] == "in_person"
    assert not upgraded['featured']
    assert isinstance(upgraded['tags'], list)

def test_validation_pipeline_error_handling():
    pipeline = ValidationPipeline()
    
    # Test invalid data
    invalid_data = {"title": ""}  # Missing required fields
    
    with pytest.raises(ValueError, match="Validation error"):
        pipeline.validate_event(invalid_data)
    
    # Test invalid schema upgrade
    with pytest.raises(ValueError, match="Schema upgrade error"):
        pipeline.upgrade_schema(invalid_data) 