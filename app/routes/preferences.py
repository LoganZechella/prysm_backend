from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from app.utils.auth import verify_session
from app.models.preferences import UserPreferences
from app.database import get_db
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter()

class PreferenceUpdate(BaseModel):
    preferred_categories: List[str]
    excluded_categories: List[str]
    min_price: float
    max_price: float
    preferred_location: Dict[str, Any]
    preferred_days: List[str]
    preferred_times: List[str]
    min_rating: float

@router.get("/")
async def get_preferences(
    session = Depends(verify_session),
    db: Session = Depends(get_db)
):
    """Get user preferences"""
    try:
        logger.debug("Handling get_preferences request")
        
        # Get user ID from session
        user_id = session.get_user_id()
        logger.debug(f"Retrieved user_id: {user_id}")
        
        # Get preferences from database
        logger.debug(f"Querying preferences for user_id: {user_id}")
        preferences = db.query(UserPreferences).filter(
            UserPreferences.user_id == user_id
        ).first()
        
        if not preferences:
            logger.debug("No preferences found, returning defaults")
            # Return default preferences
            return {
                "preferred_categories": [],
                "excluded_categories": [],
                "min_price": 0,
                "max_price": 1000,
                "preferred_location": {
                    "city": "",
                    "state": "",
                    "country": "",
                    "max_distance_km": 50
                },
                "preferred_days": [],
                "preferred_times": [],
                "min_rating": 0.5
            }
        
        logger.debug(f"Found preferences: {preferences.to_dict()}")
        return preferences.to_dict()
        
    except Exception as e:
        logger.error(f"Error getting preferences: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting preferences: {str(e)}"
        )

@router.post("/")
async def update_preferences(
    preferences: PreferenceUpdate,
    session = Depends(verify_session),
    db: Session = Depends(get_db)
):
    """Update user preferences"""
    try:
        logger.debug("Handling update_preferences request")
        logger.debug(f"Received preferences update: {preferences.model_dump()}")
        
        # Get user ID from session
        user_id = session.get_user_id()
        logger.debug(f"Retrieved user_id: {user_id}")
        
        # Get existing preferences or create new
        logger.debug(f"Querying existing preferences for user_id: {user_id}")
        user_preferences = db.query(UserPreferences).filter(
            UserPreferences.user_id == user_id
        ).first()
        
        if not user_preferences:
            logger.debug("No existing preferences found, creating new")
            user_preferences = UserPreferences(user_id=user_id)
            db.add(user_preferences)
        else:
            logger.debug("Found existing preferences, updating")
        
        # Update preferences
        user_preferences.preferred_categories = preferences.preferred_categories
        user_preferences.excluded_categories = preferences.excluded_categories
        user_preferences.min_price = preferences.min_price
        user_preferences.max_price = preferences.max_price
        user_preferences.preferred_location = preferences.preferred_location
        user_preferences.preferred_days = preferences.preferred_days
        user_preferences.preferred_times = preferences.preferred_times
        user_preferences.min_rating = preferences.min_rating
        
        # Save to database
        logger.debug("Committing changes to database")
        db.commit()
        logger.debug("Successfully updated preferences")
        
        return {"status": "success", "message": "Preferences updated"}
        
    except Exception as e:
        logger.error(f"Error updating preferences: {str(e)}", exc_info=True)
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Error updating preferences: {str(e)}"
        ) 