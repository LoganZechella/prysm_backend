import pytest
import asyncio
from datetime import datetime, timedelta
from app.collectors.eventbrite import EventbriteCollector
from app.utils.storage import StorageManager
import os
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

@pytest.mark.asyncio
async def test_eventbrite_collector():
    """Test the Eventbrite collector functionality"""
    collector = EventbriteCollector()
    
    # Test token verification
    token_valid = await collector.verify_token()
    assert token_valid is True, "Token verification failed"
    logger.info("Successfully verified token")
    
    # Test getting categories first
    categories = await collector.get_categories()
    assert len(categories) > 0, "Failed to get categories"
    logger.info(f"Successfully got {len(categories)} categories")
    
    # Test searching events using category IDs
    category_ids = [cat["id"] for cat in categories[:2]]  # Use first two categories
    response = await collector.search_events(
        location="San Francisco, CA",
        start_date=datetime.utcnow(),
        end_date=datetime.utcnow() + timedelta(days=30),
        categories=category_ids
    )
    
    assert "events" in response, "No events field in response"
    assert "pagination" in response, "No pagination field in response"
    logger.info(f"Found {len(response['events'])} events")
    
    # Test event transformation if we have any events
    if response["events"]:
        event = response["events"][0]
        transformed_event = collector.transform_event(event)
        assert transformed_event is not None, "Failed to transform event"
        logger.info(f"Successfully transformed event: {transformed_event.title}")
        
        # Test storing event
        storage = StorageManager()
        stored = storage.store_raw_event("eventbrite", transformed_event.dict())
        assert isinstance(stored, str), "store_raw_event should return a string blob name"
        logger.info("Successfully stored event")

if __name__ == "__main__":
    print("Testing Eventbrite collector...")
    print("=" * 50)
    
    success = asyncio.run(test_eventbrite_collector())
    
    print("=" * 50)
    if success:
        print("✅ All Eventbrite tests passed successfully!")
    else:
        print("❌ Eventbrite tests failed!") 