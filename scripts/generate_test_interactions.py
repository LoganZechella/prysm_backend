"""Script to generate test interaction data for Amazon Personalize setup."""
import asyncio
import logging
from datetime import datetime, timedelta
import random
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.event import EventModel
from app.models.traits import Traits
from app.models.feedback import ImplicitFeedback, UserFeedback

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def generate_test_interactions():
    """Generate test interaction data for existing users and events."""
    async with AsyncSessionLocal() as db:
        # Get all users with traits
        users_result = await db.execute(select(Traits))
        users = users_result.scalars().all()
        
        # Get all events
        events_result = await db.execute(select(EventModel))
        events = events_result.scalars().all()
        
        if not users or not events:
            logger.error("No users or events found in database")
            return
            
        logger.info(f"Found {len(users)} users and {len(events)} events")
        
        # Generate interactions for each user
        for user in users:
            # Select random events for this user to interact with
            user_events = random.sample(events, min(len(events), 5))
            
            for event in user_events:
                # Generate implicit feedback
                implicit = ImplicitFeedback(
                    user_id=user.user_id,
                    event_id=event.id,
                    view_count=random.randint(1, 5),
                    view_duration=random.uniform(30, 300),  # 30 seconds to 5 minutes
                    click_count=random.randint(0, 3),
                    share_count=random.randint(0, 1),
                    created_at=datetime.utcnow() - timedelta(days=random.randint(0, 30))
                )
                db.add(implicit)
                
                # Generate explicit feedback for some interactions (50% chance)
                if random.random() < 0.5:
                    explicit = UserFeedback(
                        user_id=user.user_id,
                        event_id=event.id,
                        rating=random.uniform(1.0, 5.0),
                        liked=random.random() < 0.7,  # 70% chance of liking
                        saved=random.random() < 0.3,  # 30% chance of saving
                        created_at=datetime.utcnow() - timedelta(days=random.randint(0, 30))
                    )
                    db.add(explicit)
            
            await db.commit()
            logger.info(f"Generated interactions for user {user.user_id}")
        
        logger.info("Finished generating test interactions")

if __name__ == '__main__':
    asyncio.run(generate_test_interactions()) 