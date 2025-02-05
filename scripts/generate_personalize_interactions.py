"""Script to generate synthetic interactions for Amazon Personalize."""
import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal
from app.models.event import EventModel
from app.models.traits import Traits
from app.models.feedback import ImplicitFeedback, UserFeedback
from app.services.personalize_transformer import PersonalizeTransformer
from app.services.personalize_monitoring import PersonalizeMonitor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InteractionGenerator:
    """Generator for synthetic interactions data."""

    def __init__(self, db: AsyncSession):
        """Initialize the generator."""
        self.db = db
        self.transformer = PersonalizeTransformer(db)
        self.monitor = PersonalizeMonitor()
        
        # Configuration
        self.num_synthetic_users = 50
        self.min_interactions_per_user = 20
        self.max_interactions_per_user = 40
        self.interaction_types = {
            'view': 0.60,  # 60% probability
            'click': 0.25, # 25% probability
            'share': 0.10, # 10% probability
            'rating': 0.05 # 5% probability
        }
        
        # User profiles for more realistic data
        self.user_profiles = [
            {'name': 'Active Social', 'view_prob': 0.5, 'click_prob': 0.3, 'share_prob': 0.15, 'rating_prob': 0.05},
            {'name': 'Content Consumer', 'view_prob': 0.7, 'click_prob': 0.2, 'share_prob': 0.05, 'rating_prob': 0.05},
            {'name': 'Event Enthusiast', 'view_prob': 0.4, 'click_prob': 0.3, 'share_prob': 0.2, 'rating_prob': 0.1},
            {'name': 'Casual Browser', 'view_prob': 0.8, 'click_prob': 0.15, 'share_prob': 0.03, 'rating_prob': 0.02}
        ]

    def _generate_user_id(self) -> str:
        """Generate a unique synthetic user ID."""
        return f"synthetic_{uuid.uuid4().hex[:8]}"

    def _generate_user_traits(self, user_id: str) -> Dict[str, Any]:
        """Generate synthetic user traits."""
        profile = random.choice(self.user_profiles)
        
        # Generate age between 18 and 65
        age = random.randint(18, 65)
        
        # Map age to age group
        if age < 25:
            age_group = "18-24"
        elif age < 35:
            age_group = "25-34"
        elif age < 45:
            age_group = "35-44"
        elif age < 55:
            age_group = "45-54"
        else:
            age_group = "55+"
            
        return {
            'user_id': user_id,
            'profile_type': profile['name'],
            'age_group': age_group,
            'gender': random.choice(['Male', 'Female', 'Other']),
            'interests': {
                'music': random.sample(['rock', 'pop', 'jazz', 'classical', 'electronic'], 2),
                'activities': random.sample(['sports', 'arts', 'technology', 'food', 'outdoor'], 2)
            },
            'behavior_patterns': {
                'view_prob': profile['view_prob'],
                'click_prob': profile['click_prob'],
                'share_prob': profile['share_prob'],
                'rating_prob': profile['rating_prob']
            }
        }

    def _generate_interaction_timestamp(self) -> datetime:
        """Generate a realistic timestamp within the past 6 months."""
        now = datetime.utcnow()
        days_ago = random.randint(0, 180)  # Past 6 months
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        
        return now - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)

    def _generate_view_duration(self) -> float:
        """Generate a realistic view duration in seconds."""
        # Most views between 30 seconds and 5 minutes
        return random.uniform(30, 300)

    def _generate_rating(self) -> float:
        """Generate a rating between 1 and 5."""
        # Bias towards positive ratings (more realistic)
        weights = [0.1, 0.15, 0.25, 0.3, 0.2]  # Probabilities for ratings 1-5
        return float(random.choices(range(1, 6), weights=weights)[0])

    async def _get_events_for_user(self, user_traits: Dict[str, Any]) -> List[EventModel]:
        """Get suitable events for a user based on their traits."""
        # Get all events
        result = await self.db.execute(select(EventModel))
        events = result.scalars().all()
        
        # Filter and score events based on user traits
        scored_events = []
        for event in events:
            score = 1.0
            
            # Boost score for matching interests
            if event.categories:
                user_interests = (
                    user_traits['interests']['music'] +
                    user_traits['interests']['activities']
                )
                matching_categories = set(event.categories) & set(user_interests)
                score += len(matching_categories) * 0.5
            
            scored_events.append((event, score))
        
        # Sort by score and return events
        scored_events.sort(key=lambda x: x[1], reverse=True)
        return [event for event, _ in scored_events]

    async def generate_interactions(self):
        """Generate synthetic interactions data."""
        try:
            # Get existing events
            result = await self.db.execute(select(EventModel))
            events = result.scalars().all()
            
            if not events:
                logger.error("No events found in database")
                return
                
            logger.info(f"Found {len(events)} events to generate interactions for")
            
            # Generate synthetic users and their interactions
            total_interactions = 0
            
            for _ in range(self.num_synthetic_users):
                user_id = self._generate_user_id()
                user_traits = self._generate_user_traits(user_id)
                
                # Get suitable events for this user
                user_events = await self._get_events_for_user(user_traits)
                
                # Generate random number of interactions for this user
                num_interactions = random.randint(
                    self.min_interactions_per_user,
                    self.max_interactions_per_user
                )
                
                # Ensure each user interacts with at least 2 different items
                selected_events = random.sample(user_events, min(len(user_events), max(2, num_interactions)))
                
                for event in selected_events:
                    # Generate 1-3 interactions per event
                    for _ in range(random.randint(1, 3)):
                        timestamp = self._generate_interaction_timestamp()
                        
                        # Determine interaction type based on user profile
                        interaction_type = random.choices(
                            list(self.interaction_types.keys()),
                            list(self.interaction_types.values())
                        )[0]
                        
                        # Create the interaction
                        if interaction_type == 'view':
                            await self.db.merge(ImplicitFeedback(
                                user_id=user_id,
                                event_id=event.id,
                                view_count=1,
                                view_duration=self._generate_view_duration(),
                                created_at=timestamp,
                                is_synthetic=True
                            ))
                        elif interaction_type == 'click':
                            await self.db.merge(ImplicitFeedback(
                                user_id=user_id,
                                event_id=event.id,
                                click_count=1,
                                created_at=timestamp,
                                is_synthetic=True
                            ))
                        elif interaction_type == 'share':
                            await self.db.merge(ImplicitFeedback(
                                user_id=user_id,
                                event_id=event.id,
                                share_count=1,
                                created_at=timestamp,
                                is_synthetic=True
                            ))
                        else:  # rating
                            await self.db.merge(UserFeedback(
                                user_id=user_id,
                                event_id=event.id,
                                rating=self._generate_rating(),
                                created_at=timestamp,
                                is_synthetic=True
                            ))
                        
                        total_interactions += 1
                
                # Commit after each user's interactions
                await self.db.commit()
                
                logger.info(f"Generated {total_interactions} interactions for user {user_id}")
            
            logger.info(f"Successfully generated {total_interactions} total interactions")
            
        except Exception as e:
            logger.error(f"Error generating interactions: {str(e)}")
            raise

async def main():
    """Main function to run the interaction generator."""
    async with AsyncSessionLocal() as db:
        generator = InteractionGenerator(db)
        await generator.generate_interactions()

if __name__ == "__main__":
    asyncio.run(main()) 