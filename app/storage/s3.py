"""S3 storage implementation for events."""

import os
import json
import aioboto3
from datetime import datetime
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError

from .base import BaseStorage

class S3Storage(BaseStorage):
    """S3 storage implementation."""
    
    def __init__(self):
        """Initialize S3 storage."""
        self.session = aioboto3.Session()
        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'prysm-events')
    
    async def initialize(self):
        """Initialize S3 storage and ensure bucket exists."""
        try:
            async with self.session.client('s3') as s3:
                try:
                    await s3.head_bucket(Bucket=self.bucket_name)
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == '404':
                        await s3.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': os.getenv('AWS_REGION', 'us-west-2')
                            }
                        )
                        
                        # Enable versioning
                        await s3.put_bucket_versioning(
                            Bucket=self.bucket_name,
                            VersioningConfiguration={'Status': 'Enabled'}
                        )
                        
                        # Enable encryption
                        await s3.put_bucket_encryption(
                            Bucket=self.bucket_name,
                            ServerSideEncryptionConfiguration={
                                'Rules': [
                                    {
                                        'ApplyServerSideEncryptionByDefault': {
                                            'SSEAlgorithm': 'AES256'
                                        }
                                    }
                                ]
                            }
                        )
                
        except Exception as e:
            print(f"Error creating S3 bucket: {str(e)}")
            raise
    
    def _get_raw_key(self, event_id: str, source: str) -> str:
        """Get S3 key for raw event data."""
        return f"raw-events/{source}/{event_id}.json"
    
    def _get_processed_key(self, event_id: str) -> str:
        """Get S3 key for processed event data."""
        return f"processed/{event_id}.json"
    
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store a batch of events in S3.
        
        Args:
            events: List of event data to store
            is_raw: Whether these are raw events
            
        Returns:
            List of stored event IDs
        """
        stored_ids = []
        
        async with self.session.client('s3') as s3:
            for event in events:
                try:
                    event_id = event['event_id']
                    source = event.get('source', 'unknown')
                    
                    # Get appropriate key
                    key = self._get_raw_key(event_id, source) if is_raw else self._get_processed_key(event_id)
                    
                    # Store event data
                    await s3.put_object(
                        Bucket=self.bucket_name,
                        Key=key,
                        Body=json.dumps(event),
                        ContentType='application/json'
                    )
                    
                    stored_ids.append(event_id)
                    
                except Exception as e:
                    print(f"Error storing event {event.get('event_id')} in S3: {str(e)}")
                    continue
        
        return stored_ids
    
    async def get_raw_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get a raw event from S3.
        
        Args:
            event_id: ID of the event to retrieve
            
        Returns:
            Raw event data or None if not found
        """
        try:
            async with self.session.client('s3') as s3:
                # List objects to find the source
                paginator = s3.get_paginator('list_objects_v2')
                prefix = f"raw-events/"
                
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        if event_id in obj['Key']:
                            response = await s3.get_object(
                                Bucket=self.bucket_name,
                                Key=obj['Key']
                            )
                            async with response['Body'] as stream:
                                data = await stream.read()
                                return json.loads(data.decode('utf-8'))
                
                return None
                
        except Exception as e:
            print(f"Error getting raw event {event_id} from S3: {str(e)}")
            return None
    
    async def list_raw_events(
        self,
        source: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """List raw events from S3.
        
        Args:
            source: Source platform to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of raw event IDs and metadata
        """
        try:
            events = []
            prefix = f"raw-events/{source}/"
            
            async with self.session.client('s3') as s3:
                paginator = s3.get_paginator('list_objects_v2')
                async for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        try:
                            # Get event data
                            response = await s3.get_object(
                                Bucket=self.bucket_name,
                                Key=obj['Key']
                            )
                            async with response['Body'] as stream:
                                data = await stream.read()
                                event_data = json.loads(data.decode('utf-8'))
                            
                            # Apply date filters
                            ingestion_time = datetime.fromisoformat(
                                event_data.get('raw_ingestion_time', '').replace('Z', '+00:00')
                            )
                            
                            if start_date and ingestion_time < start_date:
                                continue
                            if end_date and ingestion_time > end_date:
                                continue
                            
                            events.append({
                                'event_id': event_data['event_id'],
                                'source': event_data['source'],
                                'ingestion_time': event_data.get('raw_ingestion_time')
                            })
                            
                        except Exception as e:
                            print(f"Error processing S3 object {obj['Key']}: {str(e)}")
                            continue
            
            return events
            
        except Exception as e:
            print(f"Error listing raw events from S3: {str(e)}")
            return []
    
    async def store_processed_event(self, event: Dict[str, Any]) -> Optional[str]:
        """Store a processed event in S3.
        
        Args:
            event: Processed event data
            
        Returns:
            Event ID if stored successfully, None otherwise
        """
        try:
            event_id = event['event_id']
            key = self._get_processed_key(event_id)
            
            async with self.session.client('s3') as s3:
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=key,
                    Body=json.dumps(event),
                    ContentType='application/json'
                )
            
            return event_id
            
        except Exception as e:
            print(f"Error storing processed event {event.get('event_id')} in S3: {str(e)}")
            return None 