from google.cloud import storage
from google.cloud.exceptions import NotFound
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging
import asyncio
from functools import partial

logger = logging.getLogger(__name__)

class StorageManager:
    def __init__(self):
        self.client = storage.Client()
        self.raw_bucket_name = os.getenv('GCS_RAW_BUCKET')
        self.processed_bucket_name = os.getenv('GCS_PROCESSED_BUCKET')
        # Skip bucket existence check since we know they exist
        logger.info(f"Initialized StorageManager with buckets: {self.raw_bucket_name}, {self.processed_bucket_name}")

    async def store_raw_event(self, source: str, event_data: Dict[str, Any]) -> str:
        """Store raw event data in the raw bucket"""
        bucket = self.client.bucket(self.raw_bucket_name)
        
        # Create a timestamp-based path
        timestamp = datetime.utcnow().strftime('%Y/%m/%d/%H')
        blob_name = f"{source}/{timestamp}/{event_data.get('event_id', datetime.utcnow().isoformat())}.json"
        
        blob = bucket.blob(blob_name)
        
        # Run the upload in a thread pool since it's a blocking operation
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            partial(
                blob.upload_from_string,
                json.dumps(event_data),
                content_type='application/json'
            )
        )
        
        return blob_name

    async def store_processed_event(self, event_data: Dict[str, Any]) -> str:
        """Store processed event data in the processed bucket"""
        bucket = self.client.bucket(self.processed_bucket_name)
        
        # Organize by year/month for efficient querying
        start_date = datetime.fromisoformat(event_data.get('start_datetime', datetime.utcnow().isoformat()))
        blob_name = f"{start_date.year}/{start_date.month:02d}/{event_data['event_id']}.json"
        
        blob = bucket.blob(blob_name)
        
        # Run the upload in a thread pool since it's a blocking operation
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            partial(
                blob.upload_from_string,
                json.dumps(event_data),
                content_type='application/json'
            )
        )
        
        return blob_name

    async def get_raw_event(self, blob_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve raw event data"""
        bucket = self.client.bucket(self.raw_bucket_name)
        blob = bucket.blob(blob_name)
        
        try:
            # Run the download in a thread pool since it's a blocking operation
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, blob.download_as_string)
            return json.loads(content)
        except NotFound:
            logger.warning(f"Raw event not found: {blob_name}")
            return None

    async def get_processed_event(self, blob_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed event data"""
        bucket = self.client.bucket(self.processed_bucket_name)
        blob = bucket.blob(blob_name)
        
        try:
            # Run the download in a thread pool since it's a blocking operation
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, blob.download_as_string)
            return json.loads(content)
        except NotFound:
            logger.warning(f"Processed event not found: {blob_name}")
            return None

    async def list_raw_events(self, source: str, prefix: Optional[str] = None) -> List[str]:
        """List raw events for a given source"""
        bucket = self.client.bucket(self.raw_bucket_name)
        full_prefix = f"{source}/"
        if prefix:
            full_prefix += prefix
            
        # Run the list operation in a thread pool since it's a blocking operation
        loop = asyncio.get_event_loop()
        blobs = await loop.run_in_executor(None, lambda: list(bucket.list_blobs(prefix=full_prefix)))
        return [blob.name for blob in blobs]

    async def list_processed_events(self, year: Optional[int] = None, month: Optional[int] = None) -> List[str]:
        """List processed events, optionally filtered by year/month"""
        bucket = self.client.bucket(self.processed_bucket_name)
        prefix = ""
        
        if year:
            prefix = f"{year}/"
            if month:
                prefix += f"{month:02d}/"
                
        # Run the list operation in a thread pool since it's a blocking operation
        loop = asyncio.get_event_loop()
        blobs = await loop.run_in_executor(None, lambda: list(bucket.list_blobs(prefix=prefix)))
        return [blob.name for blob in blobs] 