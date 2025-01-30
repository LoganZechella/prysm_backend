"""
Service for managing the event data pipeline from scrapers to storage.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import asyncio
from functools import partial
from sqlalchemy.orm import Session

from app.storage.manager import StorageManager
from app.storage.s3 import S3Storage
from app.storage.dynamodb import DynamoDBStorage
from app.schemas.event import EventBase
from app.utils.retry_handler import RetryError
from app.models.event import EventModel
from app.services.data_quality import validate_event
from app.utils.deduplication import find_duplicate_events
from app.services.nlp_service import NLPService

logger = logging.getLogger(__name__)

class EventPipeline:
    """Manages the flow of event data from scrapers to storage"""
    
    def __init__(
        self,
        storage_manager: Optional[StorageManager] = None,
        batch_size: int = 25,
        max_retries: int = 3
    ):
        """Initialize the event pipeline"""
        self.storage = storage_manager or StorageManager()
        self.batch_size = batch_size
        self.max_retries = max_retries
        
    async def process_events(
        self,
        events: List[Dict[str, Any]],
        source: str
    ) -> List[str]:
        """
        Process and store events from scrapers.
        
        Args:
            events: List of event data from scrapers
            source: Source platform (eventbrite, facebook, meetup)
            
        Returns:
            List of processed event IDs
        """
        try:
            processed_ids = []
            
            # Process events in batches
            for i in range(0, len(events), self.batch_size):
                batch = events[i:i + self.batch_size]
                
                try:
                    # 1. Store raw events in S3
                    raw_ids = await self._store_raw_batch(batch, source)
                    
                    # 2. Process and validate events
                    processed_batch = [
                        self._process_event(event)
                        for event in batch
                    ]
                    
                    # 3. Store processed events in DynamoDB and S3
                    batch_ids = await self._store_processed_batch(processed_batch)
                    processed_ids.extend(batch_ids)
                    
                    # 4. Download and store images
                    await self._process_images(processed_batch)
                    
                except Exception as e:
                    logger.error(f"Error processing batch: {str(e)}")
                    continue
            
            return processed_ids
            
        except Exception as e:
            logger.error(f"Error in event pipeline: {str(e)}")
            return []
    
    async def _store_raw_batch(
        self,
        events: List[Dict[str, Any]],
        source: str
    ) -> List[str]:
        """Store raw events in S3"""
        try:
            # Add metadata and timestamp
            enriched_events = []
            for event in events:
                enriched_event = {
                    **event,
                    "source": source,
                    "raw_ingestion_time": datetime.utcnow().isoformat(),
                    "pipeline_version": "1.0"
                }
                enriched_events.append(enriched_event)
            
            # Store in S3
            return await self.storage.store_batch_events(enriched_events, is_raw=True)
            
        except Exception as e:
            logger.error(f"Error storing raw events: {str(e)}")
            return []
    
    def _process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate a single event"""
        try:
            # 1. Standardize dates
            if isinstance(event.get('start_time'), str):
                event['start_datetime'] = datetime.fromisoformat(
                    event['start_time'].replace('Z', '+00:00')
                )
            if isinstance(event.get('end_time'), str):
                event['end_datetime'] = datetime.fromisoformat(
                    event['end_time'].replace('Z', '+00:00')
                )
            
            # 2. Standardize location format
            location = event.get('location', {})
            if location:
                event['location'] = {
                    'venue_name': location.get('venue_name', ''),
                    'address': location.get('address', ''),
                    'city': location.get('city', ''),
                    'state': location.get('state', ''),
                    'country': location.get('country', 'US'),
                    'coordinates': {
                        'lat': float(location.get('latitude', 0)),
                        'lng': float(location.get('longitude', 0))
                    }
                }
            
            # 3. Standardize price information
            if 'price' in event or 'fee' in event:
                price_info = event.get('price', event.get('fee', {}))
                event['price_info'] = {
                    'currency': price_info.get('currency', 'USD'),
                    'min_price': float(price_info.get('amount', 0)),
                    'max_price': float(price_info.get('amount', 0)),
                    'price_tier': self._calculate_price_tier(
                        float(price_info.get('amount', 0))
                    )
                }
            
            # 4. Add processing metadata
            event['processed_at'] = datetime.utcnow().isoformat()
            event['processing_version'] = '1.0'
            
            return event
            
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}")
            return event
    
    def _calculate_price_tier(self, price: float) -> str:
        """Calculate price tier based on amount"""
        if price == 0:
            return 'free'
        elif price < 20:
            return 'budget'
        elif price < 50:
            return 'medium'
        elif price < 100:
            return 'premium'
        else:
            return 'luxury'
    
    async def _store_processed_batch(
        self,
        events: List[Dict[str, Any]]
    ) -> List[str]:
        """Store processed events in DynamoDB and S3"""
        try:
            # 1. Store in DynamoDB for fast querying
            dynamo_ids = await self.storage.store_batch_events(events)
            
            # 2. Store in S3 for durability
            s3_ids = await self.storage.store_batch_events(events, is_raw=False)
            
            # Return DynamoDB IDs as they're used for querying
            return dynamo_ids
            
        except Exception as e:
            logger.error(f"Error storing processed events: {str(e)}")
            return []
    
    async def _process_images(self, events: List[Dict[str, Any]]) -> None:
        """Download and store event images"""
        try:
            for event in events:
                if image_url := event.get('image_url'):
                    try:
                        # Download image
                        async with aiohttp.ClientSession() as session:
                            async with session.get(image_url) as response:
                                if response.status == 200:
                                    image_data = await response.read()
                                    
                                    # Store image
                                    stored_url = await self.storage.store_image(
                                        image_url,
                                        image_data,
                                        event['event_id']
                                    )
                                    
                                    # Update event with stored image URL
                                    if stored_url:
                                        await self.storage.update_event(
                                            event['event_id'],
                                            {'image_url': stored_url}
                                        )
                                        
                    except Exception as e:
                        logger.warning(f"Error processing image {image_url}: {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error in image processing: {str(e)}")
    
    async def reprocess_raw_events(
        self,
        source: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[str]:
        """Reprocess raw events from S3"""
        try:
            # 1. List raw events
            raw_events = await self.storage.list_raw_events(source, start_date, end_date)
            if not raw_events:
                return []
            
            processed_ids = []
            for raw_event in raw_events:
                try:
                    # 2. Get raw event data
                    event_data = await self.storage.get_raw_event(raw_event['event_id'])
                    if not event_data:
                        continue
                    
                    # 3. Process event
                    processed_event = self._process_event(event_data)
                    
                    # 4. Store processed event
                    event_id = await self.storage.store_processed_event(processed_event)
                    if event_id:
                        processed_ids.append(event_id)
                    
                except Exception as e:
                    logger.error(f"Error reprocessing event {raw_event['event_id']}: {str(e)}")
                    continue
            
            return processed_ids
            
        except Exception as e:
            logger.error(f"Error in reprocessing pipeline: {str(e)}")
            return [] 