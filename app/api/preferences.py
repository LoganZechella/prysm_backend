from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from app.auth import get_current_user
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
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Get user preferences"""
    try:
        logger.debug("Handling get_preferences request")
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
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Update user preferences"""
    try:
        logger.debug("Handling update_preferences request")
        logger.debug(f"Received preferences update: {preferences.model_dump()}")
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
        
        # Update preferences using setattr to handle SQLAlchemy Column types
        updates = {
            'preferred_categories': list(preferences.preferred_categories),
            'excluded_categories': list(preferences.excluded_categories),
            'min_price': float(preferences.min_price),
            'max_price': float(preferences.max_price),
            'preferred_location': dict(preferences.preferred_location),
            'preferred_days': list(preferences.preferred_days),
            'preferred_times': list(preferences.preferred_times),
            'min_rating': float(preferences.min_rating)
        }
        
        for key, value in updates.items():
            setattr(user_preferences, key, value)
        
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