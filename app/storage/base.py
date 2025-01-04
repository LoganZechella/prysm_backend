import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class StorageInterface(ABC):
    """Base interface for storage implementations"""
    
    @abstractmethod
    async def store_raw_event(self, event_data: Dict[str, Any], source: str) -> str:
        """Store raw event data and return the storage ID"""
        pass
    
    @abstractmethod
    async def store_processed_event(self, event_data: Dict[str, Any]) -> str:
        """Store processed event data and return the storage ID"""
        pass
    
    @abstractmethod
    async def get_raw_event(self, event_id: str, source: str) -> Optional[Dict[str, Any]]:
        """Retrieve raw event data by ID and source"""
        pass
    
    @abstractmethod
    async def get_processed_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve processed event data by ID"""
        pass
    
    @abstractmethod
    async def store_image(self, image_url: str, image_data: bytes, event_id: str) -> str:
        """Store event image and return the storage URL"""
        pass
    
    @abstractmethod
    async def get_image(self, image_url: str) -> Optional[bytes]:
        """Retrieve event image by URL"""
        pass
    
    @abstractmethod
    async def list_events(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        location: Optional[Dict[str, float]] = None,
        radius_km: Optional[float] = None,
        categories: Optional[List[str]] = None,
        price_range: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """List events matching the given criteria"""
        pass
    
    @abstractmethod
    async def update_event(self, event_id: str, update_data: Dict[str, Any]) -> bool:
        """Update event data, returns True if successful"""
        pass
    
    @abstractmethod
    async def delete_event(self, event_id: str) -> bool:
        """Delete event data, returns True if successful"""
        pass
    
    @abstractmethod
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store multiple events and return their storage IDs"""
        pass 