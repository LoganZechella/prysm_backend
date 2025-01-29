"""Storage utilities for managing event data."""

from typing import Dict, List, Any, Optional, Sequence
from datetime import datetime
import json
import boto3
from botocore.exceptions import ClientError
import logging
from app.config.settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

class StorageManager:
    """Manages storage operations for event data."""
    
    def __init__(self):
        """Initialize storage manager."""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        self.raw_bucket = settings.AWS_RAW_BUCKET
        self.processed_bucket = settings.AWS_PROCESSED_BUCKET
        self.batch_size = 10  # Maximum number of events to process in a single batch
    
    async def store_raw_event(self, event: Dict[str, Any], source: str) -> str:
        """
        Store raw event data in S3.
        
        Args:
            event: Raw event data
            source: Event source platform
            
        Returns:
            S3 object key
        """
        try:
            if not event.get('event_id'):
                raise ValueError("Event must have an event_id")
                
            # Add metadata
            event_data = {
                **event,
                'ingestion_timestamp': datetime.utcnow().isoformat(),
                'source': source
            }
            
            # Generate key
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            key = f"{source}/raw/{timestamp}_{event['event_id']}.json"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.raw_bucket,
                Key=key,
                Body=json.dumps(event_data)
            )
            
            return key
            
        except Exception as e:
            logger.error(f"Error storing raw event: {str(e)}")
            raise
    
    async def store_processed_event(self, event: Dict[str, Any]) -> str:
        """
        Store processed event data in S3.
        
        Args:
            event: Processed event data
            
        Returns:
            S3 object key
        """
        try:
            if not event.get('event_id'):
                raise ValueError("Event must have an event_id")
                
            # Add metadata
            event_data = {
                **event,
                'processing_timestamp': datetime.utcnow().isoformat()
            }
            
            # Generate key
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            key = f"processed/{timestamp}_{event['event_id']}.json"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.processed_bucket,
                Key=key,
                Body=json.dumps(event_data)
            )
            
            return key
            
        except Exception as e:
            logger.error(f"Error storing processed event: {str(e)}")
            raise
    
    async def store_processed_events_batch(self, events: List[Dict[str, Any]]) -> List[str]:
        """
        Store multiple processed events in batch.
        
        Args:
            events: List of processed events
            
        Returns:
            List of S3 object keys
        """
        try:
            keys = []
            
            # Process events in batches
            for i in range(0, len(events), self.batch_size):
                batch = events[i:i + self.batch_size]
                batch_keys = []
                
                # Store each event in the batch
                for event in batch:
                    key = await self.store_processed_event(event)
                    batch_keys.append(key)
                
                keys.extend(batch_keys)
            
            return keys
            
        except Exception as e:
            logger.error(f"Error storing processed events batch: {str(e)}")
            raise
    
    async def get_raw_event(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get raw event data from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            Raw event data or None if not found
        """
        try:
            if not key:
                raise ValueError("Key cannot be empty")
                
            response = self.s3_client.get_object(
                Bucket=self.raw_bucket,
                Key=key
            )
            return json.loads(response['Body'].read())
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error getting raw event: {str(e)}")
            raise
    
    async def get_processed_event(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get processed event data from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            Processed event data or None if not found
        """
        try:
            if not key:
                raise ValueError("Key cannot be empty")
                
            response = self.s3_client.get_object(
                Bucket=self.processed_bucket,
                Key=key
            )
            return json.loads(response['Body'].read())
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error getting processed event: {str(e)}")
            raise
    
    async def get_processed_events_batch(self, keys: List[str]) -> List[Optional[Dict[str, Any]]]:
        """
        Get multiple processed events in batch.
        
        Args:
            keys: List of S3 object keys
            
        Returns:
            List of event data (None for not found events)
        """
        try:
            events = []
            
            # Process keys in batches
            for i in range(0, len(keys), self.batch_size):
                batch_keys = keys[i:i + self.batch_size]
                batch_events = []
                
                # Get each event in the batch
                for key in batch_keys:
                    event = await self.get_processed_event(key)
                    batch_events.append(event)
                
                events.extend(batch_events)
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting processed events batch: {str(e)}")
            raise
    
    async def delete_processed_event(self, key: str) -> None:
        """
        Delete a processed event.
        
        Args:
            key: S3 object key
        """
        try:
            if not key:
                raise ValueError("Key cannot be empty")
                
            self.s3_client.delete_object(
                Bucket=self.processed_bucket,
                Key=key
            )
            
        except Exception as e:
            logger.error(f"Error deleting processed event: {str(e)}")
            raise
    
    async def delete_processed_events_batch(self, keys: List[str]) -> None:
        """
        Delete multiple processed events in batch.
        
        Args:
            keys: List of S3 object keys
        """
        try:
            # Process keys in batches
            for i in range(0, len(keys), self.batch_size):
                batch_keys = keys[i:i + self.batch_size]
                
                # Delete each event in the batch
                for key in batch_keys:
                    await self.delete_processed_event(key)
            
        except Exception as e:
            logger.error(f"Error deleting processed events batch: {str(e)}")
            raise
    
    async def list_raw_events(
        self,
        source: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[str]:
        """
        List raw event keys in S3.
        
        Args:
            source: Optional source platform filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of S3 object keys
        """
        try:
            prefix = f"{source}/" if source else ""
            response = self.s3_client.list_objects_v2(
                Bucket=self.raw_bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            keys = []
            for obj in response['Contents']:
                key = obj['Key']
                # Apply date filters if provided
                if start_date or end_date:
                    try:
                        # Extract date from key (format: YYYYMMDD_HHMMSS)
                        date_str = key.split('/')[-1].split('_')[0]
                        date = datetime.strptime(date_str, '%Y%m%d')
                        if start_date and date < start_date:
                            continue
                        if end_date and date > end_date:
                            continue
                    except (IndexError, ValueError):
                        continue
                keys.append(key)
            
            return keys
            
        except Exception as e:
            logger.error(f"Error listing raw events: {str(e)}")
            raise
    
    async def list_processed_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[str]:
        """
        List processed event keys in S3.
        
        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of S3 object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.processed_bucket,
                Prefix="processed/"
            )
            
            if 'Contents' not in response:
                return []
            
            keys = []
            for obj in response['Contents']:
                key = obj['Key']
                # Apply date filters if provided
                if start_date or end_date:
                    try:
                        # Extract date from key (format: YYYYMMDD_HHMMSS)
                        date_str = key.split('/')[-1].split('_')[0]
                        date = datetime.strptime(date_str, '%Y%m%d')
                        if start_date and date < start_date:
                            continue
                        if end_date and date > end_date:
                            continue
                    except (IndexError, ValueError):
                        continue
                keys.append(key)
            
            return keys
            
        except Exception as e:
            logger.error(f"Error listing processed events: {str(e)}")
            raise 