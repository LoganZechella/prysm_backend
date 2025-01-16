"""Storage manager for coordinating between DynamoDB and S3."""

from typing import Dict, Any, List, Optional
from datetime import datetime

from .dynamodb import DynamoDBStorage
from .s3 import S3Storage

class StorageManager:
    """Manages storage operations across DynamoDB and S3."""
    
    def __init__(self):
        """Initialize storage manager."""
        self.dynamodb = DynamoDBStorage()
        self.s3 = S3Storage()
        self._initialized = False
    
    async def initialize(self):
        """Initialize storage backends."""
        if not self._initialized:
            await self.s3.initialize()
            self._initialized = True
    
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store a batch of events in both DynamoDB and S3.
        
        Args:
            events: List of event data to store
            is_raw: Whether these are raw events
            
        Returns:
            List of stored event IDs
        """
        await self.initialize()
        
        # Store in both storages
        dynamo_ids = await self.dynamodb.store_batch_events(events, is_raw)
        s3_ids = await self.s3.store_batch_events(events, is_raw)
        
        # Return IDs that were successfully stored in both
        return list(set(dynamo_ids) & set(s3_ids))
    
    async def get_raw_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get a raw event from storage.
        
        Args:
            event_id: ID of the event to retrieve
            
        Returns:
            Raw event data or None if not found
        """
        await self.initialize()
        
        # Try DynamoDB first, then S3
        event = await self.dynamodb.get_raw_event(event_id)
        if not event:
            event = await self.s3.get_raw_event(event_id)
        return event
    
    async def list_raw_events(
        self,
        source: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """List raw events from storage.
        
        Args:
            source: Source platform to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of raw event IDs and metadata
        """
        await self.initialize()
        
        # Get from both storages
        dynamo_events = await self.dynamodb.list_raw_events(source, start_date, end_date)
        s3_events = await self.s3.list_raw_events(source, start_date, end_date)
        
        # Combine and deduplicate by event_id
        events_dict = {event['event_id']: event for event in dynamo_events}
        events_dict.update({event['event_id']: event for event in s3_events})
        
        return list(events_dict.values())
    
    async def store_processed_event(self, event: Dict[str, Any]) -> Optional[str]:
        """Store a processed event in both storages.
        
        Args:
            event: Processed event data
            
        Returns:
            Event ID if stored successfully, None otherwise
        """
        await self.initialize()
        
        # Store in both storages
        dynamo_id = await self.dynamodb.store_processed_event(event)
        s3_id = await self.s3.store_processed_event(event)
        
        # Return ID only if stored in both
        return dynamo_id if dynamo_id == s3_id else None 