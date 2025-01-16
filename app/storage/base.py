"""Base storage interface for event data."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime

class BaseStorage(ABC):
    """Base storage interface that all storage implementations must follow."""
    
    @abstractmethod
    async def store_batch_events(self, events: List[Dict[str, Any]], is_raw: bool = False) -> List[str]:
        """Store a batch of events.
        
        Args:
            events: List of event data to store
            is_raw: Whether these are raw events
            
        Returns:
            List of stored event IDs
        """
        pass
    
    @abstractmethod
    async def get_raw_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get a raw event.
        
        Args:
            event_id: ID of the event to retrieve
            
        Returns:
            Raw event data or None if not found
        """
        pass
    
    @abstractmethod
    async def list_raw_events(
        self,
        source: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """List raw events.
        
        Args:
            source: Source platform to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            List of raw event IDs and metadata
        """
        pass
    
    @abstractmethod
    async def store_processed_event(self, event: Dict[str, Any]) -> Optional[str]:
        """Store a processed event.
        
        Args:
            event: Processed event data
            
        Returns:
            Event ID if stored successfully, None otherwise
        """
        pass 