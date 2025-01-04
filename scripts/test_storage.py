#!/usr/bin/env python3
import asyncio
import logging
from datetime import datetime, timedelta
import json
import os
import sys

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.storage.manager import StorageManager
from app.config.storage import StorageConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_storage_infrastructure():
    """Test the storage infrastructure"""
    try:
        # Initialize storage manager
        config = StorageConfig()
        storage = StorageManager()
        
        # Create test event
        test_event = {
            "event_id": "test-event-001",
            "title": "Test Event",
            "description": "This is a test event for storage validation",
            "start_datetime": datetime.utcnow().isoformat(),
            "location": {
                "venue_name": "Test Venue",
                "address": "123 Test St",
                "city": "San Francisco",
                "state": "CA",
                "country": "USA",
                "coordinates": {
                    "lat": 37.7749,
                    "lng": -122.4194
                }
            },
            "categories": ["test", "event"],
            "price_info": {
                "currency": "USD",
                "min_price": 10.0,
                "max_price": 50.0,
                "price_tier": "medium"
            },
            "source": {
                "platform": "test",
                "url": "https://example.com/test-event",
                "last_updated": datetime.utcnow().isoformat()
            }
        }
        
        # Test storing raw event
        logger.info("Testing raw event storage...")
        raw_event_id = await storage.store_raw_event(test_event, "test")
        assert raw_event_id == test_event["event_id"]
        logger.info("Raw event storage successful")
        
        # Test retrieving raw event
        logger.info("Testing raw event retrieval...")
        retrieved_raw = await storage.get_raw_event(raw_event_id, "test")
        assert retrieved_raw is not None
        assert retrieved_raw["event_id"] == test_event["event_id"]
        logger.info("Raw event retrieval successful")
        
        # Test storing processed event
        logger.info("Testing processed event storage...")
        processed_event_id = await storage.store_processed_event(test_event)
        assert processed_event_id == test_event["event_id"]
        logger.info("Processed event storage successful")
        
        # Test retrieving processed event
        logger.info("Testing processed event retrieval...")
        retrieved_processed = await storage.get_processed_event(processed_event_id)
        assert retrieved_processed is not None
        assert retrieved_processed["event_id"] == test_event["event_id"]
        logger.info("Processed event retrieval successful")
        
        # Test storing image
        logger.info("Testing image storage...")
        test_image = b"test image data"
        image_url = "https://example.com/test-image.jpg"
        stored_url = await storage.store_image(image_url, test_image, test_event["event_id"])
        assert stored_url.startswith("s3://")
        logger.info("Image storage successful")
        
        # Test retrieving image
        logger.info("Testing image retrieval...")
        retrieved_image = await storage.get_image(stored_url)
        assert retrieved_image == test_image
        logger.info("Image retrieval successful")
        
        # Test listing events
        logger.info("Testing event listing...")
        events = await storage.list_events(
            start_date=datetime.utcnow() - timedelta(days=1),
            end_date=datetime.utcnow() + timedelta(days=1),
            categories=["test"]
        )
        assert len(events) > 0
        logger.info("Event listing successful")
        
        # Test updating event
        logger.info("Testing event update...")
        update_data = {
            "title": "Updated Test Event",
            "description": "This event has been updated"
        }
        success = await storage.update_event(test_event["event_id"], update_data)
        assert success
        
        # Verify update
        updated_event = await storage.get_processed_event(test_event["event_id"])
        assert updated_event is not None, "Updated event not found"
        assert updated_event["title"] == update_data["title"]
        logger.info("Event update successful")
        
        # Test batch operations
        logger.info("Testing batch operations...")
        batch_events = []
        for i in range(5):
            event = test_event.copy()
            event["event_id"] = f"test-event-{i:03d}"
            event["title"] = f"Test Event {i}"
            batch_events.append(event)
        
        event_ids = await storage.store_batch_events(batch_events)
        assert len(event_ids) == len(batch_events)
        logger.info("Batch operations successful")
        
        # Test deleting events
        logger.info("Testing event deletion...")
        for event_id in event_ids:
            success = await storage.delete_event(event_id)
            assert success
        logger.info("Event deletion successful")
        
        logger.info("All storage tests completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}")
        return False

def main():
    """Main entry point"""
    success = asyncio.run(test_storage_infrastructure())
    if success:
        logger.info("\nStorage infrastructure validation completed successfully!")
        sys.exit(0)
    else:
        logger.error("\nStorage infrastructure validation failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 