"""
Script to test event collection with real data and database connections.
"""

import asyncio
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from app.services.event_collection import EventCollectionService

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_event_collection():
    """Test event collection with real data"""
    
    # Check required environment variables
    required_vars = [
        'SCRAPFLY_API_KEY',
        'FACEBOOK_ACCESS_TOKEN',
        'FACEBOOK_USER_ID',
        'FACEBOOK_XS_TOKEN'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file")
        return
    
    try:
        # Initialize service
        service = EventCollectionService(scrapfly_api_key=os.getenv('SCRAPFLY_API_KEY'))
        
        # Set up test parameters
        locations = [
            {
                'city': 'Louisville',
                'state': 'KY',
                'country': 'US',
                'lat': 38.2527,
                'lng': -85.7585
            }
        ]
        
        date_range = {
            'start': datetime.now(),
            'end': datetime.now() + timedelta(days=7)
        }
        
        logger.info("Starting event collection test")
        
        # Collect events
        total_events = await service.collect_events(
            locations=locations,
            date_range=date_range
        )
        
        logger.info(f"Successfully collected {total_events} events")
        
    except Exception as e:
        logger.error(f"Error testing event collection: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    asyncio.run(test_event_collection()) 