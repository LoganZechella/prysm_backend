import logging
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from decimal import Decimal

from .base import StorageInterface

logger = logging.getLogger(__name__)

class DynamoDBStorage(StorageInterface):
    """DynamoDB storage implementation for processed events"""
    
    def __init__(self, table_name: str, aws_region: str = 'us-west-2'):
        """Initialize DynamoDB storage"""
        self.table_name = table_name
        self.dynamodb = boto3.resource('dynamodb', region_name=aws_region)
        self.table = self.dynamodb.Table(table_name)
        
        # Create table if it doesn't exist
        try:
            self.table.table_status
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self._create_table()
    
    def _create_table(self):
        """Create DynamoDB table with required indexes"""
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'event_id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'event_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'start_datetime',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'location_hash',
                    'AttributeType': 'S'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'start_datetime-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'start_datetime',
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                },
                {
                    'IndexName': 'location-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'location_hash',
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        # Wait for table to be created
        table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
    
    def _generate_location_hash(self, lat: float, lng: float, precision: int = 3) -> str:
        """Generate location hash for geospatial indexing"""
        return f"{round(lat, precision)}:{round(lng, precision)}"
    
    def _decimal_to_float(self, obj: Any) -> Any:
        """Convert Decimal objects to float for JSON serialization"""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._decimal_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._decimal_to_float(v) for v in obj]
        return obj
    
    async def store_raw_event(self, event_data: Dict[str, Any], source: str) -> str:
        """Store raw event data (not implemented for DynamoDB)"""
        raise NotImplementedError("DynamoDB storage does not support raw events")
    
    async def store_processed_event(self, event_data: Dict[str, Any]) -> str:
        """Store processed event data in DynamoDB"""
        try:
            # Generate location hash
            location = event_data.get('location', {}).get('coordinates', {})
            lat = location.get('lat', 0.0)
            lng = location.get('lng', 0.0)
            location_hash = self._generate_location_hash(lat, lng)
            
            # Add location hash and ensure datetime is string
            item = {
                **event_data,
                'location_hash': location_hash,
                'start_datetime': event_data['start_datetime'].isoformat() if isinstance(event_data['start_datetime'], datetime) else event_data['start_datetime']
            }
            
            # Store event
            self.table.put_item(Item=item)
            return event_data['event_id']
            
        except Exception as e:
            logger.error(f"Error storing processed event in DynamoDB: {str(e)}")
            raise
    
    async def get_raw_event(self, event_id: str, source: str) -> Optional[Dict[str, Any]]:
        """Get raw event data (not implemented for DynamoDB)"""
        raise NotImplementedError("DynamoDB storage does not support raw events")
    
    async def get_processed_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed event data from DynamoDB"""
        try:
            response = self.table.get_item(Key={'event_id': event_id})
            item = response.get('Item')
            if item:
                return self._decimal_to_float(item)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving processed event from DynamoDB: {str(e)}")
            return None
    
    async def store_image(self, image_url: str, image_data: bytes, event_id: str) -> str:
        """Store event image (not implemented for DynamoDB)"""
        raise NotImplementedError("DynamoDB storage does not support image storage")
    
    async def get_image(self, image_url: str) -> Optional[bytes]:
        """Retrieve event image (not implemented for DynamoDB)"""
        raise NotImplementedError("DynamoDB storage does not support image storage")
    
    async def list_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        location: Optional[Dict[str, float]] = None,
        radius_km: Optional[float] = None,
        categories: Optional[List[str]] = None,
        price_range: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """List processed events from DynamoDB"""
        try:
            filter_expression = []
            expression_values = {}
            
            # Date filter
            if start_date:
                filter_expression.append('start_datetime >= :start_date')
                expression_values[':start_date'] = start_date.isoformat()
            if end_date:
                filter_expression.append('start_datetime <= :end_date')
                expression_values[':end_date'] = end_date.isoformat()
            
            # Location filter
            if location and radius_km:
                lat = location['lat']
                lng = location['lng']
                location_hash = self._generate_location_hash(lat, lng)
                filter_expression.append('location_hash = :location_hash')
                expression_values[':location_hash'] = location_hash
            
            # Category filter
            if categories:
                filter_expression.append('contains(categories, :category)')
                expression_values[':category'] = categories[0]  # Simple implementation
            
            # Price filter
            if price_range:
                if 'min' in price_range:
                    filter_expression.append('price_info.max_price >= :min_price')
                    expression_values[':min_price'] = Decimal(str(price_range['min']))
                if 'max' in price_range:
                    filter_expression.append('price_info.min_price <= :max_price')
                    expression_values[':max_price'] = Decimal(str(price_range['max']))
            
            # Build query parameters
            scan_kwargs = {}
            if filter_expression:
                scan_kwargs['FilterExpression'] = ' AND '.join(filter_expression)
                scan_kwargs['ExpressionAttributeValues'] = expression_values
            
            # Scan table with filters
            response = self.table.scan(**scan_kwargs)
            events = response.get('Items', [])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = self.table.scan(**scan_kwargs)
                events.extend(response.get('Items', []))
            
            return [self._decimal_to_float(event) for event in events]
            
        except Exception as e:
            logger.error(f"Error listing events from DynamoDB: {str(e)}")
            return []
    
    async def update_event(self, event_id: str, update_data: Dict[str, Any]) -> bool:
        """Update event data in DynamoDB"""
        try:
            # Build update expression
            update_expr = []
            expr_values = {}
            expr_names = {}
            
            for key, value in update_data.items():
                if key != 'event_id':  # Skip primary key
                    update_expr.append(f"#{key} = :{key}")
                    expr_names[f"#{key}"] = key
                    expr_values[f":{key}"] = value
            
            if not update_expr:
                return False
            
            # Update item
            self.table.update_item(
                Key={'event_id': event_id},
                UpdateExpression='SET ' + ', '.join(update_expr),
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating event in DynamoDB: {str(e)}")
            return False
    
    async def delete_event(self, event_id: str) -> bool:
        """Delete event data from DynamoDB"""
        try:
            self.table.delete_item(Key={'event_id': event_id})
            return True
            
        except Exception as e:
            logger.error(f"Error deleting event from DynamoDB: {str(e)}")
            return False
    
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store multiple events in DynamoDB"""
        if is_raw:
            raise NotImplementedError("DynamoDB storage does not support raw events")
        
        try:
            event_ids = []
            with self.table.batch_writer() as batch:
                for event in events:
                    # Generate location hash
                    location = event.get('location', {}).get('coordinates', {})
                    lat = location.get('lat', 0.0)
                    lng = location.get('lng', 0.0)
                    location_hash = self._generate_location_hash(lat, lng)
                    
                    # Add location hash and ensure datetime is string
                    item = {
                        **event,
                        'location_hash': location_hash,
                        'start_datetime': event['start_datetime'].isoformat() if isinstance(event['start_datetime'], datetime) else event['start_datetime']
                    }
                    
                    batch.put_item(Item=item)
                    event_ids.append(event['event_id'])
            
            return event_ids
            
        except Exception as e:
            logger.error(f"Error storing batch events in DynamoDB: {str(e)}")
            return [] 