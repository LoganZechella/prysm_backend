from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Dict, Any, Optional, List
from datetime import datetime
from app.services.event_collection import EventCollectionService
from app.auth import get_current_user
import os

router = APIRouter()

@router.post("/collect")
async def trigger_event_collection(
    background_tasks: BackgroundTasks,
    locations: Optional[List[Dict[str, Any]]] = None,
    date_range: Optional[Dict[str, datetime]] = None,
    _: str = Depends(get_current_user)  # Require authentication
) -> Dict[str, str]:
    """
    Trigger event collection task.
    
    Args:
        locations: Optional list of locations to search
        date_range: Optional date range to search
        
    Returns:
        Status message
    """
    try:
        # Get API key from environment
        scrapfly_api_key = os.getenv("SCRAPFLY_API_KEY")
        if not scrapfly_api_key:
            raise HTTPException(
                status_code=500,
                detail="Scrapfly API key not configured"
            )
            
        # Initialize service
        service = EventCollectionService(scrapfly_api_key=scrapfly_api_key)
        
        # Add collection task to background tasks
        background_tasks.add_task(
            service.collect_events,
            locations=locations,
            date_range=date_range
        )
        
        return {"status": "Event collection started in background"}
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start event collection: {str(e)}"
        ) 