"""DynamoDB storage implementation for events."""

import os
import json
import aioboto3
from datetime import datetime
from typing import Dict, Any, List, Optional

from .base import BaseStorage

class DynamoDBStorage(BaseStorage):
    """DynamoDB storage implementation."""
    
    def __init__(self):
        """Initialize DynamoDB storage."""
        self.session = aioboto3.Session()
        self.events_table_name = os.getenv('DYNAMODB_EVENTS_TABLE', 'prysm-events')
        self.raw_events_table_name = os.getenv('DYNAMODB_RAW_EVENTS_TABLE', 'prysm-raw-events')
    
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store a batch of events in DynamoDB.
        
        Args:
            events: List of event data to store
            is_raw: Whether these are raw events
            
        Returns:
            List of stored event IDs
        """
        stored_ids = []
        table_name = self.raw_events_table_name if is_raw else self.events_table_name
        
        dynamodb = await self.session.resource('dynamodb')
        table = await dynamodb.Table(table_name)
        
        # DynamoDB has a limit of 25 items per batch write
        for i in range(0, len(events), 25):
            batch = events[i:i + 25]
            batch_writer = await table.batch_writer()
            
            async with batch_writer as writer:
                for event in batch:
                    try:
                        if is_raw:
                            # Store raw event
                            item = {
                                'event_id': event['event_id'],
                                'source': event['source'],
                                'raw_data': json.dumps(event),
                                'ingestion_time': event.get('raw_ingestion_time', datetime.utcnow().isoformat()),
                                'pipeline_version': event.get('pipeline_version', '1.0')
                            }
                        else:
                            # Store processed event
                            item = {
                                'event_id': event['event_id'],
                                'source': event.get('source', 'unknown'),
                                'title': event.get('title', ''),
                                'description': event.get('description', ''),
                                'start_datetime': event.get('start_datetime', ''),
                                'end_datetime': event.get('end_datetime', ''),
                                'location': event.get('location', {}),
                                'price_info': event.get('price_info', {}),
                                'image_url': event.get('image_url', ''),
                                'url': event.get('url', ''),
                                'processed_at': event.get('processed_at', datetime.utcnow().isoformat()),
                                'processing_version': event.get('processing_version', '1.0')
                            }
                        
                        await writer.put_item(Item=item)
                        stored_ids.append(event['event_id'])
                        
                    except Exception as e:
                        # Log error but continue with other events
                        print(f"Error storing event {event.get('event_id')}: {str(e)}")
                        continue
        
        return stored_ids
    
    async def get_raw_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get a raw event from DynamoDB.
        
        Args:
            event_id: ID of the event to retrieve
            
        Returns:
            Raw event data or None if not found
        """
        try:
            dynamodb = await self.session.resource('dynamodb')
            table = await dynamodb.Table(self.raw_events_table_name)
            response = await table.get_item(Key={'event_id': event_id})
            if 'Item' in response:
                item = response['Item']
                return json.loads(item['raw_data'])
            return None
        except Exception as e:
            print(f"Error getting raw event {event_id}: {str(e)}")
            return None
    
    async def list_raw_events(
        self,
        source: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """List raw events from DynamoDB.
        
        Args:
            source: Source platform to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of raw event IDs and metadata
        """
        try:
            dynamodb = await self.session.resource('dynamodb')
            table = await dynamodb.Table(self.raw_events_table_name)
            
            # Build filter expression
            filter_expr = "source = :source"
            expr_values = {':source': source}
            
            if start_date:
                filter_expr += " AND ingestion_time >= :start"
                expr_values[':start'] = start_date.isoformat()
            if end_date:
                filter_expr += " AND ingestion_time <= :end"
                expr_values[':end'] = end_date.isoformat()
            
            # Query raw events
            response = await table.scan(
                FilterExpression=filter_expr,
                ExpressionAttributeValues=expr_values
            )
            
            events = []
            for item in response.get('Items', []):
                events.append({
                    'event_id': item['event_id'],
                    'source': item['source'],
                    'ingestion_time': item['ingestion_time']
                })
            
            return events
                
        except Exception as e:
            print(f"Error listing raw events: {str(e)}")
            return []
    
    async def store_processed_event(self, event: Dict[str, Any]) -> Optional[str]:
        """Store a processed event in DynamoDB.
        
        Args:
            event: Processed event data
            
        Returns:
            Event ID if stored successfully, None otherwise
        """
        try:
            dynamodb = await self.session.resource('dynamodb')
            table = await dynamodb.Table(self.events_table_name)
            
            item = {
                'event_id': event['event_id'],
                'source': event.get('source', 'unknown'),
                'title': event.get('title', ''),
                'description': event.get('description', ''),
                'start_datetime': event.get('start_datetime', ''),
                'end_datetime': event.get('end_datetime', ''),
                'location': event.get('location', {}),
                'price_info': event.get('price_info', {}),
                'image_url': event.get('image_url', ''),
                'url': event.get('url', ''),
                'processed_at': event.get('processed_at', datetime.utcnow().isoformat()),
                'processing_version': event.get('processing_version', '1.0')
            }
            
            await table.put_item(Item=item)
            return event['event_id']
                
        except Exception as e:
            print(f"Error storing processed event {event.get('event_id')}: {str(e)}")
            return None 