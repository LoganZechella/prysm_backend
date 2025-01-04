import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from .s3 import S3Storage
from .dynamodb import DynamoDBStorage
from ..config.storage import StorageConfig

logger = logging.getLogger(__name__)

class StorageManager:
    """Manager for handling both S3 and DynamoDB storage"""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize storage manager with configuration"""
        self.config = StorageConfig(config_path)
        
        # Initialize storage instances
        self.s3 = S3Storage(
            self.config.get_s3_bucket_name(),
            self.config.get_s3_region()
        )
        self.dynamodb = DynamoDBStorage(
            self.config.get_dynamodb_table_name(),
            self.config.get_dynamodb_region()
        )
    
    async def store_raw_event(self, event_data: Dict[str, Any], source: str) -> str:
        """Store raw event data in S3"""
        return await self.s3.store_raw_event(event_data, source)
    
    async def store_processed_event(self, event_data: Dict[str, Any]) -> str:
        """Store processed event in both S3 and DynamoDB"""
        # Store in S3 first
        s3_event_id = await self.s3.store_processed_event(event_data)
        
        try:
            # Then store in DynamoDB
            dynamodb_event_id = await self.dynamodb.store_processed_event(event_data)
            
            if s3_event_id != dynamodb_event_id:
                logger.warning(
                    f"Event ID mismatch: S3={s3_event_id}, DynamoDB={dynamodb_event_id}"
                )
            
            return s3_event_id
            
        except Exception as e:
            logger.error(f"Error storing event in DynamoDB: {str(e)}")
            # Continue with S3 ID even if DynamoDB fails
            return s3_event_id
    
    async def get_raw_event(self, event_id: str, source: str) -> Optional[Dict[str, Any]]:
        """Retrieve raw event data from S3"""
        return await self.s3.get_raw_event(event_id, source)
    
    async def get_processed_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed event from DynamoDB, fallback to S3"""
        try:
            # Try DynamoDB first
            event = await self.dynamodb.get_processed_event(event_id)
            if event:
                return event
            
            # Fallback to S3 if configured
            if self.config.get_fallback_strategy() == 's3':
                return await self.s3.get_processed_event(event_id)
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving event from DynamoDB: {str(e)}")
            # Fallback to S3 if configured
            if self.config.get_fallback_strategy() == 's3':
                return await self.s3.get_processed_event(event_id)
            return None
    
    async def store_image(self, image_url: str, image_data: bytes, event_id: str) -> str:
        """Store event image in S3"""
        return await self.s3.store_image(image_url, image_data, event_id)
    
    async def get_image(self, image_url: str) -> Optional[bytes]:
        """Retrieve event image from S3"""
        return await self.s3.get_image(image_url)
    
    async def list_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        location: Optional[Dict[str, float]] = None,
        radius_km: Optional[float] = None,
        categories: Optional[List[str]] = None,
        price_range: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """List processed events from DynamoDB, fallback to S3"""
        try:
            # Try DynamoDB first
            events = await self.dynamodb.list_events(
                start_date=start_date,
                end_date=end_date,
                location=location,
                radius_km=radius_km,
                categories=categories,
                price_range=price_range
            )
            
            if events:
                return events
            
            # Fallback to S3 if configured
            if self.config.get_fallback_strategy() == 's3':
                return await self.s3.list_events(
                    start_date=start_date,
                    end_date=end_date,
                    location=location,
                    radius_km=radius_km,
                    categories=categories,
                    price_range=price_range
                )
            
            return []
            
        except Exception as e:
            logger.error(f"Error listing events from DynamoDB: {str(e)}")
            # Fallback to S3 if configured
            if self.config.get_fallback_strategy() == 's3':
                return await self.s3.list_events(
                    start_date=start_date,
                    end_date=end_date,
                    location=location,
                    radius_km=radius_km,
                    categories=categories,
                    price_range=price_range
                )
            return []
    
    async def update_event(self, event_id: str, update_data: Dict[str, Any]) -> bool:
        """Update event in both S3 and DynamoDB"""
        success = True
        
        try:
            # Update in S3
            s3_success = await self.s3.update_event(event_id, update_data)
            if not s3_success:
                success = False
                logger.error(f"Failed to update event {event_id} in S3")
        except Exception as e:
            success = False
            logger.error(f"Error updating event in S3: {str(e)}")
        
        try:
            # Update in DynamoDB
            dynamodb_success = await self.dynamodb.update_event(event_id, update_data)
            if not dynamodb_success:
                success = False
                logger.error(f"Failed to update event {event_id} in DynamoDB")
        except Exception as e:
            success = False
            logger.error(f"Error updating event in DynamoDB: {str(e)}")
        
        return success
    
    async def delete_event(self, event_id: str) -> bool:
        """Delete event from both S3 and DynamoDB"""
        success = True
        
        try:
            # Delete from S3
            s3_success = await self.s3.delete_event(event_id)
            if not s3_success:
                success = False
                logger.error(f"Failed to delete event {event_id} from S3")
        except Exception as e:
            success = False
            logger.error(f"Error deleting event from S3: {str(e)}")
        
        try:
            # Delete from DynamoDB
            dynamodb_success = await self.dynamodb.delete_event(event_id)
            if not dynamodb_success:
                success = False
                logger.error(f"Failed to delete event {event_id} from DynamoDB")
        except Exception as e:
            success = False
            logger.error(f"Error deleting event from DynamoDB: {str(e)}")
        
        return success
    
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store multiple events in both S3 and DynamoDB"""
        if is_raw:
            # Store raw events only in S3
            return await self.s3.store_batch_events(events, is_raw=True)
        
        try:
            # Get batch size from config
            batch_size = self.config.get_batch_size()
            
            # Process events in batches
            event_ids = []
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                
                # Store batch in S3
                s3_batch_ids = await self.s3.store_batch_events(batch)
                event_ids.extend(s3_batch_ids)
                
                try:
                    # Store batch in DynamoDB
                    dynamodb_batch_ids = await self.dynamodb.store_batch_events(batch)
                    
                    if set(s3_batch_ids) != set(dynamodb_batch_ids):
                        logger.warning(
                            "Event ID mismatch between S3 and DynamoDB batch storage"
                        )
                    
                except Exception as e:
                    logger.error(f"Error storing batch in DynamoDB: {str(e)}")
            
            return event_ids
            
        except Exception as e:
            logger.error(f"Error storing batch events in S3: {str(e)}")
            return [] 