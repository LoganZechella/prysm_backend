import os
import logging
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv
from app.collectors.meta import MetaCollector

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Testing Meta collector...")
print("=" * 50)

@pytest.mark.asyncio
async def test_meta_collector():
    # Initialize collector
    collector = MetaCollector()
    
    # Test token verification
    token_valid = await collector.verify_token()
    assert token_valid, "Token verification failed"
    logger.info("Successfully verified token")
    
    # Test event search
    location = "37.7749,-122.4194"  # San Francisco
    start_date = datetime.utcnow()
    end_date = start_date + timedelta(days=30)
    
    events = await collector.search_events(
        location=location,
        start_date=start_date,
        end_date=end_date
    )
    
    # Log the number of events found
    num_events = len(events.get("data", []))
    logger.info(f"Found {num_events} events")
    
    # Even if no events are found, the API calls should work
    assert isinstance(events, dict), "Events response should be a dictionary"
    assert "data" in events, "Events response should have a 'data' key"
    assert isinstance(events["data"], list), "Events data should be a list"
    
    if num_events == 0:
        logger.warning("No events found in the search results")
    else:
        # Test event transformation
        event = events["data"][0]
        transformed = collector.transform_event(event)
        assert transformed.title, "Transformed event should have a title"
        assert transformed.source.platform == "meta", "Event source platform should be 'meta'"

if __name__ == "__main__":
    pytest.main([__file__]) 