"""Script to verify and prepare interaction data for Amazon Personalize."""
import asyncio
import logging
from datetime import datetime
from collections import defaultdict
from typing import Dict, Set, List
import pandas as pd
from sqlalchemy import select, func
import json

from app.db.session import AsyncSessionLocal
from app.models.feedback import ImplicitFeedback, UserFeedback
from app.services.personalize_transformer import PersonalizeTransformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersonalizeDataVerifier:
    """Verify and prepare data for Amazon Personalize."""

    def __init__(self, db):
        """Initialize the verifier."""
        self.db = db
        self.transformer = PersonalizeTransformer(db)
        
        # Track metrics
        self.metrics = {
            'total_interactions': 0,
            'unique_users': set(),
            'unique_items': set(),
            'interaction_types': defaultdict(int),
            'interactions_per_user': defaultdict(int),
            'interactions_per_item': defaultdict(int),
            'synthetic_ratio': 0.0
        }

    async def collect_metrics(self):
        """Collect metrics about the interaction data."""
        # Query implicit feedback
        implicit_result = await self.db.execute(select(ImplicitFeedback))
        implicit_feedback = implicit_result.scalars().all()
        
        # Query explicit feedback
        explicit_result = await self.db.execute(select(UserFeedback))
        explicit_feedback = explicit_result.scalars().all()
        
        # Process implicit feedback
        synthetic_count = 0
        for feedback in implicit_feedback:
            if feedback.is_synthetic:
                synthetic_count += 1
                
            if feedback.view_count > 0:
                self._track_interaction(feedback.user_id, feedback.event_id, 'view')
            if feedback.click_count > 0:
                self._track_interaction(feedback.user_id, feedback.event_id, 'click')
            if feedback.share_count > 0:
                self._track_interaction(feedback.user_id, feedback.event_id, 'share')
        
        # Process explicit feedback
        for feedback in explicit_feedback:
            if feedback.is_synthetic:
                synthetic_count += 1
            if feedback.rating:
                self._track_interaction(feedback.user_id, feedback.event_id, 'rating')
        
        # Calculate synthetic ratio
        total = len(implicit_feedback) + len(explicit_feedback)
        self.metrics['synthetic_ratio'] = (synthetic_count / total * 100) if total > 0 else 0

    def _track_interaction(self, user_id: str, event_id: int, interaction_type: str):
        """Track an interaction in the metrics."""
        self.metrics['total_interactions'] += 1
        self.metrics['unique_users'].add(user_id)
        self.metrics['unique_items'].add(event_id)
        self.metrics['interaction_types'][interaction_type] += 1
        self.metrics['interactions_per_user'][user_id] += 1
        self.metrics['interactions_per_item'][event_id] += 1

    def verify_requirements(self) -> List[str]:
        """Verify if the data meets Amazon Personalize requirements.
        
        Returns:
            List of requirement violations, empty if all requirements are met.
        """
        violations = []
        
        # Check total interactions
        if self.metrics['total_interactions'] < 1000:
            violations.append(
                f"Insufficient total interactions: {self.metrics['total_interactions']} < 1000"
            )
        
        # Check unique users
        if len(self.metrics['unique_users']) < 25:
            violations.append(
                f"Insufficient unique users: {len(self.metrics['unique_users'])} < 25"
            )
        
        # Check interactions per user
        users_with_few_interactions = sum(
            1 for count in self.metrics['interactions_per_user'].values() if count < 2
        )
        if users_with_few_interactions > 0:
            violations.append(
                f"{users_with_few_interactions} users have fewer than 2 interactions"
            )
        
        # Check interactions per item
        items_with_few_interactions = sum(
            1 for count in self.metrics['interactions_per_item'].values() if count < 2
        )
        if items_with_few_interactions > 0:
            violations.append(
                f"{items_with_few_interactions} items have fewer than 2 interactions"
            )
        
        return violations

    def print_metrics(self):
        """Print the collected metrics."""
        logger.info("\nInteraction Data Metrics:")
        logger.info("-" * 50)
        logger.info(f"Total Interactions: {self.metrics['total_interactions']}")
        logger.info(f"Unique Users: {len(self.metrics['unique_users'])}")
        logger.info(f"Unique Items: {len(self.metrics['unique_items'])}")
        logger.info(f"Synthetic Data Ratio: {self.metrics['synthetic_ratio']:.1f}%")
        
        logger.info("\nInteraction Types:")
        for itype, count in self.metrics['interaction_types'].items():
            percentage = (count / self.metrics['total_interactions']) * 100
            logger.info(f"- {itype}: {count} ({percentage:.1f}%)")
        
        logger.info("\nInteractions per User:")
        user_interactions = list(self.metrics['interactions_per_user'].values())
        logger.info(f"- Min: {min(user_interactions)}")
        logger.info(f"- Max: {max(user_interactions)}")
        logger.info(f"- Avg: {sum(user_interactions) / len(user_interactions):.1f}")
        
        logger.info("\nInteractions per Item:")
        item_interactions = list(self.metrics['interactions_per_item'].values())
        logger.info(f"- Min: {min(item_interactions)}")
        logger.info(f"- Max: {max(item_interactions)}")
        logger.info(f"- Avg: {sum(item_interactions) / len(item_interactions):.1f}")

async def main():
    """Main function to verify Personalize data."""
    async with AsyncSessionLocal() as db:
        verifier = PersonalizeDataVerifier(db)
        
        # Collect and verify metrics
        await verifier.collect_metrics()
        verifier.print_metrics()
        
        # Check requirements
        violations = verifier.verify_requirements()
        if violations:
            logger.error("\nRequirement Violations:")
            for violation in violations:
                logger.error(f"- {violation}")
        else:
            logger.info("\nAll Amazon Personalize requirements are met! ✅")

if __name__ == "__main__":
    asyncio.run(main()) 