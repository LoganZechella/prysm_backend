"""Service for tracking user interactions with events."""
import json
import logging
import boto3
from datetime import datetime
from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.feedback import ImplicitFeedback, UserFeedback
from app.models.event import EventModel
from app.core.config import settings

logger = logging.getLogger(__name__)

class InteractionTracker:
    """Service for tracking user interactions with events."""
    
    def __init__(self, db: AsyncSession):
        """Initialize the interaction tracker."""
        self.db = db
        self.personalize_events = boto3.client(
            'personalize-events',
            region_name=settings.AWS_DEFAULT_REGION
        )
    
    async def track_interaction(
        self,
        user_id: str,
        event_id: str,
        event_type: str,
        event_value: Optional[float] = None,
        session_id: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Track a user interaction with an event.
        
        Args:
            user_id: The ID of the user
            event_id: The ID of the event
            event_type: Type of interaction (e.g., 'click', 'view', 'rsvp')
            event_value: Optional value associated with the event (e.g., rating)
            session_id: Optional session ID (will be generated if not provided)
            properties: Optional additional properties to track
        """
        try:
            # Generate session ID if not provided
            if not session_id:
                session_id = f"session-{user_id}-{datetime.utcnow().timestamp()}"
            
            # Prepare event properties
            event_properties = {
                "itemId": str(event_id),
                "eventValue": float(event_value) if event_value is not None else None
            }
            
            # Add additional properties if provided
            if properties:
                event_properties.update(properties)
            
            # Remove None values from properties
            event_properties = {k: v for k, v in event_properties.items() if v is not None}
            
            # Send event to Personalize
            self.personalize_events.put_events(
                trackingId=settings.AWS_PERSONALIZE_TRACKING_ID,
                userId=str(user_id),
                sessionId=session_id,
                eventList=[{
                    "eventId": f"{user_id}-{event_id}-{datetime.utcnow().timestamp()}",
                    "sentAt": datetime.utcnow(),
                    "eventType": event_type,
                    "properties": json.dumps(event_properties)
                }]
            )
            
            logger.info(
                f"Tracked {event_type} interaction for user {user_id} "
                f"on event {event_id}"
            )
            
        except Exception as e:
            logger.error(
                f"Error tracking {event_type} interaction for user {user_id} "
                f"on event {event_id}: {str(e)}"
            )
            raise
    
    async def track_view(
        self,
        user_id: str,
        event_id: str,
        duration: Optional[float] = None
    ) -> None:
        """Track an event view interaction."""
        await self.track_interaction(
            user_id=user_id,
            event_id=event_id,
            event_type="view",
            event_value=duration,
            properties={"interaction_type": "view"}
        )
    
    async def track_click(
        self,
        user_id: str,
        event_id: str
    ) -> None:
        """Track an event click interaction."""
        await self.track_interaction(
            user_id=user_id,
            event_id=event_id,
            event_type="click",
            properties={"interaction_type": "click"}
        )
    
    async def track_rsvp(
        self,
        user_id: str,
        event_id: str,
        response: str
    ) -> None:
        """Track an RSVP interaction."""
        await self.track_interaction(
            user_id=user_id,
            event_id=event_id,
            event_type="rsvp",
            properties={
                "interaction_type": "rsvp",
                "response": response
            }
        )
    
    async def track_rating(
        self,
        user_id: str,
        event_id: str,
        rating: float
    ) -> None:
        """Track an event rating interaction."""
        await self.track_interaction(
            user_id=user_id,
            event_id=event_id,
            event_type="rating",
            event_value=rating,
            properties={"interaction_type": "rating"}
        )
    
    async def track_bookmark(
        self,
        user_id: str,
        event_id: str,
        action: str
    ) -> None:
        """Track a bookmark interaction."""
        await self.track_interaction(
            user_id=user_id,
            event_id=event_id,
            event_type="bookmark",
            properties={
                "interaction_type": "bookmark",
                "action": action
            }
        )

class InteractionTrackingService:
    """Service for tracking and managing user interactions with events."""
    
    def __init__(self, db: AsyncSession):
        """Initialize the interaction tracking service."""
        self.db = db
    
    async def track_view(
        self,
        user_id: str,
        event_id: int,
        view_duration: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> ImplicitFeedback:
        """
        Track when a user views an event.
        
        Args:
            user_id: The ID of the user
            event_id: The ID of the event being viewed
            view_duration: Optional duration of the view in seconds
            context: Optional additional context about the view
        """
        # Get existing implicit feedback or create new
        feedback = await self._get_or_create_implicit_feedback(user_id, event_id)
        
        # Update view count and duration
        feedback.view_count += 1
        if view_duration:
            feedback.view_duration += view_duration
            
        if context:
            feedback.feedback_context = {
                **(feedback.feedback_context or {}),
                **context
            }
        
        await self.db.commit()
        return feedback
    
    async def track_click(
        self,
        user_id: str,
        event_id: int,
        context: Optional[Dict[str, Any]] = None
    ) -> ImplicitFeedback:
        """
        Track when a user clicks on an event.
        
        Args:
            user_id: The ID of the user
            event_id: The ID of the event being clicked
            context: Optional additional context about the click
        """
        # Get existing implicit feedback or create new
        feedback = await self._get_or_create_implicit_feedback(user_id, event_id)
        
        # Update click count
        feedback.click_count += 1
            
        if context:
            feedback.feedback_context = {
                **(feedback.feedback_context or {}),
                **context
            }
        
        await self.db.commit()
        return feedback
    
    async def track_rsvp(
        self,
        user_id: str,
        event_id: int,
        rsvp_status: bool,
        context: Optional[Dict[str, Any]] = None
    ) -> UserFeedback:
        """
        Track when a user RSVPs to an event.
        
        Args:
            user_id: The ID of the user
            event_id: The ID of the event
            rsvp_status: True if attending, False if not
            context: Optional additional context about the RSVP
        """
        # Get existing feedback or create new
        feedback = await self._get_or_create_user_feedback(user_id, event_id)
        
        # Update RSVP status (stored in the 'saved' field)
        feedback.saved = rsvp_status
            
        if context:
            feedback.feedback_context = {
                **(feedback.feedback_context or {}),
                **context,
                'rsvp_timestamp': datetime.utcnow().isoformat()
            }
        
        await self.db.commit()
        return feedback
    
    async def get_user_interactions(
        self,
        user_id: str,
        interaction_type: str = 'all'
    ) -> List[Dict[str, Any]]:
        """
        Get all interactions for a user.
        
        Args:
            user_id: The ID of the user
            interaction_type: Type of interactions to get ('implicit', 'explicit', or 'all')
        """
        interactions = []
        
        if interaction_type in ['implicit', 'all']:
            implicit = await self.db.execute(
                select(ImplicitFeedback)
                .filter_by(user_id=user_id)
            )
            implicit_feedback = implicit.scalars().all()
            interactions.extend([f.to_dict() for f in implicit_feedback])
            
        if interaction_type in ['explicit', 'all']:
            explicit = await self.db.execute(
                select(UserFeedback)
                .filter_by(user_id=user_id)
            )
            explicit_feedback = explicit.scalars().all()
            interactions.extend([f.to_dict() for f in explicit_feedback])
            
        return interactions
    
    async def _get_or_create_implicit_feedback(
        self,
        user_id: str,
        event_id: int
    ) -> ImplicitFeedback:
        """Get existing implicit feedback or create new one."""
        result = await self.db.execute(
            select(ImplicitFeedback)
            .filter_by(user_id=user_id, event_id=event_id)
        )
        feedback = result.scalar_one_or_none()
        
        if not feedback:
            feedback = ImplicitFeedback(
                user_id=user_id,
                event_id=event_id,
                view_count=0,
                view_duration=0.0,
                click_count=0,
                share_count=0
            )
            self.db.add(feedback)
            
        return feedback
    
    async def _get_or_create_user_feedback(
        self,
        user_id: str,
        event_id: int
    ) -> UserFeedback:
        """Get existing user feedback or create new one."""
        result = await self.db.execute(
            select(UserFeedback)
            .filter_by(user_id=user_id, event_id=event_id)
        )
        feedback = result.scalar_one_or_none()
        
        if not feedback:
            feedback = UserFeedback(
                user_id=user_id,
                event_id=event_id
            )
            self.db.add(feedback)
            
        return feedback 