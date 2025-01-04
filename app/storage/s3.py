import logging
import json
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
from urllib.parse import urlparse
import io

from .base import StorageInterface
from app.config.aws import AWSConfig

logger = logging.getLogger(__name__)

class S3Storage(StorageInterface):
    """S3 storage implementation for raw data and images"""
    
    def __init__(self, bucket_name: str, aws_config: Optional[AWSConfig] = None):
        """Initialize S3 storage"""
        self.aws_config = aws_config or AWSConfig()
        self.bucket_name = bucket_name or self.aws_config.get_s3_bucket_name()
        
        # Initialize S3 client with credentials
        credentials = self.aws_config.get_credentials()
        self.s3_client = boto3.client(
            's3',
            region_name=self.aws_config.get_region(),
            aws_access_key_id=credentials['access_key_id'],
            aws_secret_access_key=credentials['secret_access_key'],
            aws_session_token=credentials.get('session_token')
        )
        
        # Create bucket if it doesn't exist
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.aws_config.get_region()
                    }
                )
                
                # Configure bucket based on settings
                s3_config = self.aws_config.get_s3_config()
                
                # Enable versioning if configured
                if s3_config.get('versioning') == 'enabled':
                    self.s3_client.put_bucket_versioning(
                        Bucket=self.bucket_name,
                        VersioningConfiguration={'Status': 'Enabled'}
                    )
                
                # Set lifecycle rules if configured
                lifecycle_rules = s3_config.get('lifecycle_rules', {})
                if lifecycle_rules:
                    self.s3_client.put_bucket_lifecycle_configuration(
                        Bucket=self.bucket_name,
                        LifecycleConfiguration={
                            'Rules': [
                                {
                                    'ID': 'raw-data-expiration',
                                    'Status': 'Enabled',
                                    'Prefix': 'raw/',
                                    'Expiration': {
                                        'Days': lifecycle_rules.get('raw_data_retention_days', 90)
                                    }
                                },
                                {
                                    'ID': 'processed-data-expiration',
                                    'Status': 'Enabled',
                                    'Prefix': 'processed/',
                                    'Expiration': {
                                        'Days': lifecycle_rules.get('processed_data_retention_days', 30)
                                    }
                                }
                            ]
                        }
                    )
                
                # Configure CORS if specified
                cors_config = s3_config.get('cors', {})
                if cors_config:
                    self.s3_client.put_bucket_cors(
                        Bucket=self.bucket_name,
                        CORSConfiguration={
                            'CORSRules': [
                                {
                                    'AllowedOrigins': cors_config.get('allowed_origins', ['*']),
                                    'AllowedMethods': cors_config.get('allowed_methods', ['GET', 'PUT', 'POST']),
                                    'AllowedHeaders': cors_config.get('allowed_headers', ['*']),
                                    'MaxAgeSeconds': cors_config.get('max_age_seconds', 3600)
                                }
                            ]
                        }
                    )
    
    def _generate_key(self, prefix: str, identifier: str) -> str:
        """Generate S3 key with prefix and identifier"""
        return f"{prefix}/{identifier}"
    
    def _generate_image_key(self, image_url: str, event_id: str) -> str:
        """Generate unique key for image storage"""
        url_hash = hashlib.md5(image_url.encode()).hexdigest()
        ext = self._get_file_extension(image_url)
        return f"images/{event_id}/{url_hash}{ext}"
    
    def _get_file_extension(self, url: str) -> str:
        """Get file extension from URL"""
        path = urlparse(url).path
        return path[path.rfind('.'):] if '.' in path else ''
    
    async def store_raw_event(self, event_data: Dict[str, Any], source: str) -> str:
        """Store raw event data in S3"""
        try:
            event_id = event_data.get('id', '')
            key = self._generate_key(f"raw/{source}", event_id)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(event_data),
                ContentType='application/json'
            )
            
            return event_id
            
        except Exception as e:
            logger.error(f"Error storing raw event in S3: {str(e)}")
            raise
    
    async def store_processed_event(self, event_data: Dict[str, Any]) -> str:
        """Store processed event data in S3"""
        try:
            event_id = event_data.get('event_id', '')
            key = self._generate_key('processed', event_id)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(event_data),
                ContentType='application/json'
            )
            
            return event_id
            
        except Exception as e:
            logger.error(f"Error storing processed event in S3: {str(e)}")
            raise
    
    async def get_raw_event(self, event_id: str, source: str) -> Optional[Dict[str, Any]]:
        """Retrieve raw event data from S3"""
        try:
            key = self._generate_key(f"raw/{source}", event_id)
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            return json.loads(response['Body'].read())
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error retrieving raw event from S3: {str(e)}")
            return None
    
    async def get_processed_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed event data from S3"""
        try:
            key = self._generate_key('processed', event_id)
            
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            return json.loads(response['Body'].read())
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error retrieving processed event from S3: {str(e)}")
            return None
    
    async def store_image(self, image_url: str, image_data: bytes, event_id: str) -> str:
        """Store event image in S3"""
        try:
            key = self._generate_image_key(image_url, event_id)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=image_data,
                ContentType='image/jpeg'  # Assuming JPEG, could be made dynamic
            )
            
            return f"s3://{self.bucket_name}/{key}"
            
        except Exception as e:
            logger.error(f"Error storing image in S3: {str(e)}")
            raise
    
    async def get_image(self, image_url: str) -> Optional[bytes]:
        """Retrieve event image from S3"""
        try:
            if not image_url.startswith('s3://'):
                return None
            
            # Parse S3 URL
            parts = urlparse(image_url)
            bucket = parts.netloc
            key = parts.path.lstrip('/')
            
            response = self.s3_client.get_object(
                Bucket=bucket,
                Key=key
            )
            
            return response['Body'].read()
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error retrieving image from S3: {str(e)}")
            return None
    
    async def list_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        location: Optional[Dict[str, float]] = None,
        radius_km: Optional[float] = None,
        categories: Optional[List[str]] = None,
        price_range: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """List processed events from S3"""
        try:
            # List all processed events
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix='processed/'
            )
            
            events = []
            for page in pages:
                for obj in page.get('Contents', []):
                    response = self.s3_client.get_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key']
                    )
                    event = json.loads(response['Body'].read())
                    
                    # Apply filters
                    if self._matches_filters(
                        event,
                        start_date,
                        end_date,
                        location,
                        radius_km,
                        categories,
                        price_range
                    ):
                        events.append(event)
            
            return events
            
        except Exception as e:
            logger.error(f"Error listing events from S3: {str(e)}")
            return []
    
    def _matches_filters(
        self,
        event: Dict[str, Any],
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        location: Optional[Dict[str, float]],
        radius_km: Optional[float],
        categories: Optional[List[str]],
        price_range: Optional[Dict[str, float]]
    ) -> bool:
        """Check if event matches the given filters"""
        # Date filter
        if start_date and event['start_datetime'] < start_date.isoformat():
            return False
        if end_date and event['start_datetime'] > end_date.isoformat():
            return False
        
        # Location filter (simplified distance check)
        if location and radius_km:
            event_loc = event['location']['coordinates']
            if not self._within_radius(
                event_loc['lat'],
                event_loc['lng'],
                location['lat'],
                location['lng'],
                radius_km
            ):
                return False
        
        # Category filter
        if categories and not any(cat in event['categories'] for cat in categories):
            return False
        
        # Price filter
        if price_range:
            price_info = event['price_info']
            if (
                price_info['min_price'] > price_range.get('max', float('inf')) or
                price_info['max_price'] < price_range.get('min', 0)
            ):
                return False
        
        return True
    
    def _within_radius(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        radius_km: float
    ) -> bool:
        """Simple distance check using bounding box"""
        # Approximate degrees per km
        km_per_deg = 111.0
        
        lat_diff = abs(lat1 - lat2)
        lon_diff = abs(lon1 - lon2)
        
        return (
            lat_diff * km_per_deg <= radius_km and
            lon_diff * km_per_deg <= radius_km
        )
    
    async def update_event(self, event_id: str, update_data: Dict[str, Any]) -> bool:
        """Update event data in S3"""
        try:
            # Get existing event
            existing_event = await self.get_processed_event(event_id)
            if not existing_event:
                return False
            
            # Update event data
            existing_event.update(update_data)
            
            # Store updated event
            await self.store_processed_event(existing_event)
            return True
            
        except Exception as e:
            logger.error(f"Error updating event in S3: {str(e)}")
            return False
    
    async def delete_event(self, event_id: str) -> bool:
        """Delete event data from S3"""
        try:
            # Delete processed event
            key = self._generate_key('processed', event_id)
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            # Delete associated images
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=f"images/{event_id}/"
            )
            
            for page in pages:
                for obj in page.get('Contents', []):
                    self.s3_client.delete_object(
                        Bucket=self.bucket_name,
                        Key=obj['Key']
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting event from S3: {str(e)}")
            return False
    
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store multiple events in S3"""
        try:
            event_ids = []
            for event in events:
                if is_raw:
                    source = event.get('source', {}).get('platform', 'unknown')
                    event_id = await self.store_raw_event(event, source)
                else:
                    event_id = await self.store_processed_event(event)
                event_ids.append(event_id)
            return event_ids
            
        except Exception as e:
            logger.error(f"Error storing batch events in S3: {str(e)}")
            return [] 