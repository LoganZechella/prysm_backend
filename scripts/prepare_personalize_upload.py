"""Script to prepare and upload interaction data to S3 for Amazon Personalize."""
import asyncio
import logging
import os
from datetime import datetime
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import select

from app.db.session import AsyncSessionLocal
from app.models.feedback import ImplicitFeedback, UserFeedback
from app.models.event import EventModel
from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersonalizeDataPreparer:
    """Prepare and upload interaction data for Amazon Personalize."""

    def __init__(self, db):
        """Initialize the preparer."""
        self.db = db
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_DEFAULT_REGION
        )

    async def get_interaction_data(self):
        """Fetch and transform interaction data into Personalize format."""
        # Query all feedback data
        implicit_result = await self.db.execute(select(ImplicitFeedback))
        explicit_result = await self.db.execute(select(UserFeedback))
        
        implicit_feedback = implicit_result.scalars().all()
        explicit_feedback = explicit_result.scalars().all()

        interactions = []

        # Process implicit feedback
        for feedback in implicit_feedback:
            timestamp = int(feedback.created_at.timestamp())
            
            # Add view interactions
            if feedback.view_count > 0:
                interactions.append({
                    'USER_ID': feedback.user_id,
                    'ITEM_ID': str(feedback.event_id),
                    'TIMESTAMP': timestamp,
                    'EVENT_TYPE': 'view',
                    'EVENT_VALUE': feedback.view_count
                })
            
            # Add click interactions
            if feedback.click_count > 0:
                interactions.append({
                    'USER_ID': feedback.user_id,
                    'ITEM_ID': str(feedback.event_id),
                    'TIMESTAMP': timestamp,
                    'EVENT_TYPE': 'click',
                    'EVENT_VALUE': feedback.click_count
                })
            
            # Add share interactions
            if feedback.share_count > 0:
                interactions.append({
                    'USER_ID': feedback.user_id,
                    'ITEM_ID': str(feedback.event_id),
                    'TIMESTAMP': timestamp,
                    'EVENT_TYPE': 'share',
                    'EVENT_VALUE': feedback.share_count
                })

        # Process explicit feedback
        for feedback in explicit_feedback:
            if feedback.rating:
                interactions.append({
                    'USER_ID': feedback.user_id,
                    'ITEM_ID': str(feedback.event_id),
                    'TIMESTAMP': int(feedback.created_at.timestamp()),
                    'EVENT_TYPE': 'rating',
                    'EVENT_VALUE': feedback.rating
                })

        return pd.DataFrame(interactions)

    def save_to_csv(self, df: pd.DataFrame, filename: str = 'interactions.csv'):
        """Save the interaction data to a CSV file."""
        filepath = f'data/{filename}'
        os.makedirs('data', exist_ok=True)
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved interaction data to {filepath}")
        return filepath

    def upload_to_s3(self, filepath: str, bucket: str, s3_key: str):
        """Upload the CSV file to S3."""
        try:
            self.s3_client.upload_file(filepath, bucket, s3_key)
            logger.info(f"Successfully uploaded {filepath} to s3://{bucket}/{s3_key}")
            return f"s3://{bucket}/{s3_key}"
        except ClientError as e:
            logger.error(f"Error uploading to S3: {e}")
            raise

async def main():
    """Main function to prepare and upload Personalize data."""
    async with AsyncSessionLocal() as db:
        preparer = PersonalizeDataPreparer(db)
        
        # Get and transform data
        logger.info("Fetching and transforming interaction data...")
        df = await preparer.get_interaction_data()
        logger.info(f"Transformed {len(df)} interactions")
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'personalize_interactions_{timestamp}.csv'
        filepath = preparer.save_to_csv(df, filename)
        
        # Upload to S3
        bucket = f"{settings.S3_BUCKET_PREFIX}-personalize"
        s3_key = f'interactions/{filename}'
        
        try:
            s3_uri = preparer.upload_to_s3(filepath, bucket, s3_key)
            logger.info("\nNext steps:")
            logger.info("1. Use this S3 URI in your Personalize dataset import job:")
            logger.info(f"   {s3_uri}")
            logger.info("2. Ensure your Personalize service role has access to this S3 location")
            logger.info("3. Create a dataset import job in Personalize using this URI")
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 