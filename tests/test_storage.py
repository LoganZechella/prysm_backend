import pytest
from datetime import datetime
from app.utils.storage import StorageManager
from app.utils.schema import Event, Location, PriceInfo, SourceInfo
import json

def create_test_event():
    """Create a sample event for testing"""
    return Event(
        event_id="test-event-001",
        title="Test Event",
        description="This is a test event for storage validation",
        start_datetime=datetime.utcnow(),
        location=Location(
            venue_name="Test Venue",
            address="123 Test St",
            city="San Francisco",
            state="CA",
            country="USA",
            coordinates={"lat": 37.7749, "lng": -122.4194}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=10.0,
            max_price=50.0,
            price_tier="medium"
        ),
        source=SourceInfo(
            platform="test",
            url="https://example.com/test-event"
        )
    )

def test_storage_system():
    """Test the complete storage workflow"""
    # Initialize storage manager
    storage = StorageManager()
    
    # Create test event
    event = create_test_event()
    
    try:
        # Convert event to dict with proper datetime handling
        event_dict = json.loads(event.json())
        
        # Test storing raw event
        raw_blob_name = storage.store_raw_event("test", event_dict)
        print(f"Stored raw event: {raw_blob_name}")
        
        # Test retrieving raw event
        retrieved_raw = storage.get_raw_event(raw_blob_name)
        assert retrieved_raw is not None
        print("Successfully retrieved raw event")
        
        # Test storing processed event
        processed_blob_name = storage.store_processed_event(event_dict)
        print(f"Stored processed event: {processed_blob_name}")
        
        # Test retrieving processed event
        retrieved_processed = storage.get_processed_event(processed_blob_name)
        assert retrieved_processed is not None
        print("Successfully retrieved processed event")
        
        # Test listing events
        raw_events = storage.list_raw_events("test")
        assert len(raw_events) > 0
        print(f"Listed {len(raw_events)} raw events")
        
        # Test event schema validation
        retrieved_event = Event.from_raw_data(retrieved_processed)
        assert retrieved_event.event_id == event.event_id
        print("Event schema validation successful")
        
        return True
        
    except Exception as e:
        print(f"Error during testing: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_storage_system()
    if success:
        print("\nAll storage tests passed successfully!")
    else:
        print("\nStorage tests failed!") 