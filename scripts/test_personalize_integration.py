"""Test script for Amazon Personalize integration."""
import asyncio
import logging
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.services.personalize_transformer import PersonalizeTransformer
from app.services.personalize_monitoring import PersonalizeMonitor
from app.models.event import EventModel
from app.models.traits import Traits

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_personalize_integration():
    """Test the Personalize integration with enhanced monitoring."""
    async with AsyncSessionLocal() as db:
        transformer = PersonalizeTransformer(db)
        monitor = PersonalizeMonitor()
        
        # Get all events and users
        result = await db.execute(select(EventModel))
        events = result.scalars().all()
        
        result = await db.execute(select(Traits))
        users = result.scalars().all()
        
        logger.info(f"Found {len(events)} events and {len(users)} users to process")
        
        # Process events
        for event in events:
            try:
                # Transform event data
                event_data = await transformer.transform_event_data(event)
                
                # Track transformation result
                if event_data:
                    monitor.track_transform(str(event.id), True)
                    
                    # Track field statistics
                    for field, value in event_data.items():
                        monitor.track_field_stats(field, value)
                    
                    # Track specific metrics
                    monitor.track_category(event_data['CATEGORY_L1'])
                    monitor.track_price(event_data['PRICE'])
                else:
                    monitor.track_transform(str(event.id), False, "Transformation failed")
                    
            except Exception as e:
                monitor.track_transform(str(event.id), False, str(e))
                logger.error(f"Error processing event {event.id}: {str(e)}")
        
        # Log the monitoring report
        monitor.log_report()
        
        logger.info("\nPersonalize integration test complete!")

if __name__ == "__main__":
    asyncio.run(test_personalize_integration()) 