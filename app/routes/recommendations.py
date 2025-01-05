from fastapi import APIRouter, HTTPException, Request, Depends
from typing import List, Dict, Any
from app.utils.auth import verify_session
from app.utils.recommendation import RecommendationEngine
from app.database import get_db
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter()
recommendation_engine = RecommendationEngine()

@router.get("")
async def get_recommendations(
    request: Request,
    db: Session = Depends(get_db)
):
    """Get personalized event recommendations for the current user."""
    try:
        # Verify session and get user ID
        session = await verify_session(request)
        user_id = session.get_user_id()
        
        # Mock recommendations for now
        mock_recommendations = {
            "recommendations": {
                "events": [
                    {
                        "event_id": "1",
                        "title": "Sample Event 1",
                        "description": "This is a sample event description",
                        "start_datetime": "2024-03-20T19:00:00Z",
                        "location": {
                            "venue_name": "Sample Venue",
                            "city": "San Francisco"
                        },
                        "categories": ["Music", "Live Performance"],
                        "price_info": {
                            "min_price": 25.0,
                            "max_price": 50.0,
                            "price_tier": "mid"
                        },
                        "match_score": 0.85
                    },
                    {
                        "event_id": "2",
                        "title": "Sample Event 2",
                        "description": "Another sample event description",
                        "start_datetime": "2024-03-21T20:00:00Z",
                        "location": {
                            "venue_name": "Another Venue",
                            "city": "San Francisco"
                        },
                        "categories": ["Art", "Exhibition"],
                        "price_info": {
                            "min_price": 15.0,
                            "max_price": 15.0,
                            "price_tier": "low"
                        },
                        "match_score": 0.75
                    }
                ],
                "network": []
            },
            "user_id": user_id,
            "timestamp": "2024-03-19T00:00:00Z"
        }
        
        return mock_recommendations
            
    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get recommendations"
        ) 