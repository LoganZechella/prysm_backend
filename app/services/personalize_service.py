"""Service for interacting with Amazon Personalize."""
import json
import boto3
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.event import EventModel
from app.models.traits import Traits
from app.models.feedback import ImplicitFeedback, UserFeedback
from app.core.config import settings
from app.services.personalize_transformer import PersonalizeTransformer

logger = logging.getLogger(__name__)

class PersonalizeService:
    """Service for interacting with Amazon Personalize."""
    
    def __init__(self, db: AsyncSession):
        """Initialize the Personalize service."""
        self.db = db
        self.personalize = boto3.client('personalize', region_name=settings.AWS_DEFAULT_REGION)
        self.personalize_runtime = boto3.client('personalize-runtime', region_name=settings.AWS_DEFAULT_REGION)
        self.personalize_events = boto3.client('personalize-events', region_name=settings.AWS_DEFAULT_REGION)
        self.transformer = PersonalizeTransformer(db)
        
        # Schema definitions for ECOMMERCE domain
        self.user_schema = {
            "type": "record",
            "name": "Users",
            "namespace": "com.amazonaws.personalize.schema",
            "fields": [
                {"name": "USER_ID", "type": "string"},
                {"name": "MEMBERSHIP_STATUS", "type": ["null", "string"]},  # Required for ECOMMERCE domain
                {"name": "AGE", "type": ["null", "int"]},  # Required for ECOMMERCE domain
                {"name": "GENDER", "type": ["null", "string"]},  # Required for ECOMMERCE domain
                {"name": "INTERESTS", "type": ["null", "string"]},  # Custom metadata
                {"name": "PROFESSIONAL_FIELD", "type": ["null", "string"]},  # Custom metadata
                {"name": "BEHAVIOR_PATTERNS", "type": ["null", "string"]}  # Custom metadata
            ]
        }
        
        self.item_schema = {
            "type": "record",
            "name": "Items",
            "namespace": "com.amazonaws.personalize.schema",
            "fields": [
                {"name": "ITEM_ID", "type": "string"},
                {"name": "CATEGORY", "type": ["null", "string"]},  # Required for ECOMMERCE domain
                {"name": "PRICE", "type": ["null", "float"]},  # Required for ECOMMERCE domain
                {"name": "PRODUCT_DESCRIPTION", "type": ["null", "string"]},  # Required for ECOMMERCE domain
                {"name": "VENUE_CITY", "type": ["null", "string"]},  # Custom metadata
                {"name": "VENUE_STATE", "type": ["null", "string"]},  # Custom metadata
                {"name": "IS_ONLINE", "type": ["null", "boolean"]},  # Custom metadata
                {"name": "TECHNICAL_LEVEL", "type": ["null", "float"]},  # Custom metadata
                {"name": "CREATION_TIMESTAMP", "type": "long"}
            ]
        }
        
        self.interactions_schema = {
            "type": "record",
            "name": "Interactions",
            "namespace": "com.amazonaws.personalize.schema",
            "fields": [
                {"name": "USER_ID", "type": "string"},
                {"name": "ITEM_ID", "type": "string"},
                {"name": "EVENT_TYPE", "type": "string"},
                {"name": "EVENT_VALUE", "type": ["null", "float"]},
                {"name": "TIMESTAMP", "type": "long"}
            ]
        }
    
    async def transform_user_data(self, user_id: str) -> Dict[str, Any]:
        """Transform user data into Personalize format."""
        # Get user traits
        result = await self.db.execute(
            select(Traits).filter_by(user_id=user_id)
        )
        traits = result.scalar_one_or_none()
        
        if not traits:
            return None
            
        # Extract traits
        music_traits = traits.music_traits or {}
        social_traits = traits.social_traits or {}
        professional_traits = traits.professional_traits or {}
        behavior_traits = traits.behavior_traits or {}
        
        # Map age to age group
        age = professional_traits.get('age')
        age_group = "Unknown"  # Default value for required field
        if age is not None:
            age = int(age)
            if age < 18:
                age_group = "Under 18"
            elif age < 25:
                age_group = "18-24"
            elif age < 35:
                age_group = "25-34"
            elif age < 45:
                age_group = "35-44"
            elif age < 55:
                age_group = "45-54"
            else:
                age_group = "55+"
        
        # Combine all interests
        all_interests = {
            **music_traits.get('genres', {}),
            **social_traits.get('interests', {}),
            **professional_traits.get('interests', {})
        }
        
        return {
            "USER_ID": user_id,
            "GENDER": social_traits.get('gender', 'Unknown'),  # Default value for required field
            "AGE_GROUP": age_group,
            "INTERESTS": json.dumps(all_interests),
            "PROFESSIONAL_FIELD": json.dumps(professional_traits),
            "BEHAVIOR_PATTERNS": json.dumps(behavior_traits)
        }
    
    async def transform_event_data(self, event: EventModel) -> Dict[str, Any]:
        """Transform event data into Personalize format."""
        # Handle price info safely
        min_price = 0.0  # Default value for required field
        if isinstance(event.price_info, dict):
            min_price = float(event.price_info.get("min_price", 0))
        elif isinstance(event.price_info, str):
            try:
                price_info = json.loads(event.price_info)
                min_price = float(price_info.get("min_price", 0))
            except (json.JSONDecodeError, AttributeError):
                min_price = 0.0
        elif isinstance(event.price_info, list) and len(event.price_info) > 0:
            # If it's a list, try to get the minimum price from the first item
            try:
                if isinstance(event.price_info[0], dict):
                    min_price = float(event.price_info[0].get("min_price", 0))
                else:
                    min_price = float(event.price_info[0])
            except (ValueError, AttributeError, IndexError):
                min_price = 0.0
        
        # Get primary and secondary categories
        categories = event.categories or ["Other"]
        category_l1 = categories[0] if len(categories) > 0 else "Other"  # Default value for required field
        category_l2 = categories[1] if len(categories) > 1 else "Other"  # Default value for required field
        
        return {
            "ITEM_ID": str(event.id),
            "CATEGORY_L1": category_l1,
            "CATEGORY_L2": category_l2,
            "PRICE": min_price,
            "DESCRIPTION": event.description or "No description available",  # Default value for required field
            "VENUE_CITY": event.venue_city,
            "VENUE_STATE": event.venue_state,
            "IS_ONLINE": event.is_online,
            "TECHNICAL_LEVEL": event.technical_level,
            "CREATION_TIMESTAMP": int(event.created_at.timestamp())
        }
    
    async def transform_interaction_data(
        self,
        user_id: str,
        event_id: int,
        event_type: str,
        event_value: Optional[float] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Transform interaction data into Personalize format."""
        return {
            "USER_ID": user_id,
            "ITEM_ID": str(event_id),
            "EVENT_TYPE": event_type,
            "EVENT_VALUE": event_value,
            "TIMESTAMP": int((timestamp or datetime.utcnow()).timestamp())
        }
    
    async def record_event(
        self,
        user_id: str,
        event_id: int,
        event_type: str,
        event_value: Optional[float] = None
    ) -> None:
        """Record an event in Amazon Personalize."""
        try:
            if not hasattr(settings, 'AWS_PERSONALIZE_TRACKING_ID'):
                logger.warning("AWS_PERSONALIZE_TRACKING_ID not set, skipping event recording")
                return
                
            # Transform the interaction data
            interaction = await self.transformer.transform_interaction_data(
                user_id=user_id,
                event_id=str(event_id),
                event_type=event_type,
                event_value=event_value
            )
            
            # Record the event in Personalize
            self.personalize_events.put_events(
                trackingId=settings.AWS_PERSONALIZE_TRACKING_ID,
                userId=user_id,
                sessionId=f"session-{user_id}",
                eventList=[
                    {
                        "eventId": f"{user_id}-{event_id}-{datetime.utcnow().timestamp()}",
                        "sentAt": datetime.utcnow(),
                        "eventType": event_type,
                        "properties": json.dumps(interaction)
                    }
                ]
            )
            
            logger.info(f"Recorded event {event_type} for user {user_id} on event {event_id}")
            
        except Exception as e:
            logger.error(f"Error recording event in Personalize: {str(e)}")
            # Don't raise the exception, as this is a non-critical operation
    
    async def get_recommendations(
        self,
        user_id: str,
        num_results: int = 10,
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get personalized recommendations for a user."""
        try:
            if not hasattr(settings, 'AWS_PERSONALIZE_RECOMMENDER_ARN'):
                logger.warning("AWS_PERSONALIZE_RECOMMENDER_ARN not set, skipping recommendation")
                return []
                
            # Get recommendations from Personalize
            response = self.personalize_runtime.get_recommendations(
                recommenderArn=settings.AWS_PERSONALIZE_RECOMMENDER_ARN,
                userId=user_id,
                numResults=num_results,
                context=context or {}
            )
            
            return response.get('itemList', [])
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            return []
    
    async def get_similar_items(
        self,
        item_id: str,
        num_results: int = 10,
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get similar items recommendations."""
        try:
            if not hasattr(settings, 'AWS_PERSONALIZE_RECOMMENDER_ARN'):
                logger.warning("AWS_PERSONALIZE_RECOMMENDER_ARN not set, skipping similar items")
                return []
                
            # Get similar items from Personalize
            response = self.personalize_runtime.get_recommendations(
                recommenderArn=settings.AWS_PERSONALIZE_RECOMMENDER_ARN,
                itemId=item_id,
                numResults=num_results,
                context=context or {}
            )
            
            return response.get('itemList', [])
            
        except Exception as e:
            logger.error(f"Error getting similar items: {str(e)}")
            return []
    
    async def batch_get_recommendations(
        self,
        user_ids: List[str],
        num_results: int = 25,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[str]]:
        """Get recommendations for multiple users."""
        recommendations = {}
        for user_id in user_ids:
            recommendations[user_id] = await self.get_recommendations(
                user_id=user_id,
                num_results=num_results,
                context=context
            )
        return recommendations 