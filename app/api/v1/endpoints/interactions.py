"""API endpoints for tracking user interactions with events."""
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import get_db
from app.services.recommendation import RecommendationService
from app.models.feedback import ImplicitFeedback, UserFeedback

router = APIRouter()

@router.post("/track/view/{event_id}")
async def track_view(
    event_id: int,
    user_id: str,
    view_duration: Optional[float] = None,
    context: Optional[Dict[str, Any]] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Track when a user views an event.
    
    Args:
        event_id: ID of the event being viewed
        user_id: ID of the user viewing the event
        view_duration: Optional duration of the view in seconds
        context: Optional additional context about the view
    """
    try:
        recommendation_service = RecommendationService(db)
        await recommendation_service.track_interaction(
            user_id=user_id,
            event_id=event_id,
            interaction_type='view',
            context={
                **(context or {}),
                'view_duration': view_duration
            }
        )
        return {"status": "success", "message": "View tracked successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error tracking view: {str(e)}"
        )

@router.post("/track/click/{event_id}")
async def track_click(
    event_id: int,
    user_id: str,
    context: Optional[Dict[str, Any]] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Track when a user clicks on an event.
    
    Args:
        event_id: ID of the event being clicked
        user_id: ID of the user clicking the event
        context: Optional additional context about the click
    """
    try:
        recommendation_service = RecommendationService(db)
        await recommendation_service.track_interaction(
            user_id=user_id,
            event_id=event_id,
            interaction_type='click',
            context=context
        )
        return {"status": "success", "message": "Click tracked successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error tracking click: {str(e)}"
        )

@router.post("/track/rsvp/{event_id}")
async def track_rsvp(
    event_id: int,
    user_id: str,
    rsvp_status: bool = True,
    context: Optional[Dict[str, Any]] = None,
    db: AsyncSession = Depends(get_db)
):
    """
    Track when a user RSVPs to an event.
    
    Args:
        event_id: ID of the event
        user_id: ID of the user
        rsvp_status: True if attending, False if not
        context: Optional additional context about the RSVP
    """
    try:
        recommendation_service = RecommendationService(db)
        await recommendation_service.track_interaction(
            user_id=user_id,
            event_id=event_id,
            interaction_type='rsvp',
            context={
                **(context or {}),
                'rsvp_status': rsvp_status
            }
        )
        return {"status": "success", "message": "RSVP tracked successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error tracking RSVP: {str(e)}"
        )

@router.get("/interactions/{user_id}")
async def get_user_interactions(
    user_id: str,
    interaction_type: str = 'all',
    db: AsyncSession = Depends(get_db)
):
    """
    Get all interactions for a user.
    
    Args:
        user_id: ID of the user
        interaction_type: Type of interactions to get ('implicit', 'explicit', or 'all')
    """
    try:
        recommendation_service = RecommendationService(db)
        interactions = await recommendation_service.interaction_service.get_user_interactions(
            user_id=user_id,
            interaction_type=interaction_type
        )
        return {"interactions": interactions}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error getting user interactions: {str(e)}"
        ) 