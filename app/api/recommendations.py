from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from datetime import datetime, timedelta
from app.services.recommendation import RecommendationService
from app.models.event import EventModel
from app.models.preferences import UserPreferences
from app.schemas.event import EventResponse
from app.database import get_db
from sqlalchemy.orm import Session
from app.auth import get_current_user
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

def create_sample_events(db: Session) -> None:
    """Create sample events if none exist."""
    if db.query(EventModel).count() == 0:
        sample_events = [
            EventModel(
                title="Tech Conference 2025",
                description="Join us for the biggest tech conference of the year!",
                start_time=datetime.utcnow() + timedelta(days=7),
                end_time=datetime.utcnow() + timedelta(days=7, hours=8),
                location={"lat": 37.7749, "lng": -122.4194},
                categories=["technology", "business", "education"],
                price_info={"min_price": 99.99, "max_price": 299.99, "currency": "USD"},
                source="eventbrite",
                source_id="tech_conf_2025",
                url="https://example.com/tech-conf",
                image_url="https://example.com/tech-conf.jpg",
                venue={"name": "Convention Center", "address": "123 Main St, San Francisco, CA"},
                organizer={"name": "Tech Events Inc", "description": "Leading tech event organizer"},
                tags=["AI", "blockchain", "cloud"],
                view_count=1000,
                like_count=500
            ),
            EventModel(
                title="Summer Music Festival",
                description="A weekend of amazing live music performances!",
                start_time=datetime.utcnow() + timedelta(days=14),
                end_time=datetime.utcnow() + timedelta(days=16),
                location={"lat": 34.0522, "lng": -118.2437},
                categories=["music", "entertainment", "outdoor"],
                price_info={"min_price": 149.99, "max_price": 499.99, "currency": "USD"},
                source="ticketmaster",
                source_id="summer_fest_2025",
                url="https://example.com/summer-fest",
                image_url="https://example.com/summer-fest.jpg",
                venue={"name": "Central Park", "address": "456 Park Ave, Los Angeles, CA"},
                organizer={"name": "Music Events Co", "description": "Premier music festival organizer"},
                tags=["live music", "festival", "summer"],
                view_count=2000,
                like_count=1500
            ),
            EventModel(
                title="Food & Wine Expo",
                description="Experience the finest cuisines and wines from around the world!",
                start_time=datetime.utcnow() + timedelta(days=21),
                end_time=datetime.utcnow() + timedelta(days=21, hours=6),
                location={"lat": 40.7128, "lng": -74.0060},
                categories=["food", "culture", "social"],
                price_info={"min_price": 79.99, "max_price": 199.99, "currency": "USD"},
                source="eventbrite",
                source_id="food_expo_2025",
                url="https://example.com/food-expo",
                image_url="https://example.com/food-expo.jpg",
                venue={"name": "Grand Hall", "address": "789 Food St, New York, NY"},
                organizer={"name": "Culinary Events", "description": "Gourmet event specialists"},
                tags=["wine tasting", "gourmet", "networking"],
                view_count=1500,
                like_count=800
            )
        ]
        
        for event in sample_events:
            db.add(event)
        db.commit()
        logger.info("Created sample events")

@router.get("")
async def get_recommendations(
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """
    Get recommendations for the current user. Defaults to personalized recommendations.
    """
    try:
        # Create sample events if none exist
        create_sample_events(db)
        
        # Get user preferences
        preferences = db.query(UserPreferences).filter(
            UserPreferences.user_id == user_id
        ).first()
        
        if not preferences:
            preferences = UserPreferences(user_id=user_id)
            
        # Get available events
        events = db.query(EventModel).all()
        
        # Initialize recommendation service with db session
        recommendation_service = RecommendationService(db)
        
        # Generate recommendations
        recommendations = await recommendation_service.get_personalized_recommendations(
            preferences=preferences,
            events=events,
            limit=page_size,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "recommendations": {
                "events": recommendations,
                "network": []  # Empty for now, can be populated later
            },
            "user_id": user_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get recommendations"
        )

@router.get("/personalized", response_model=List[EventResponse])
async def get_personalized_recommendations(
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100)
) -> List[EventModel]:
    """
    Get personalized event recommendations for the current user.
    """
    try:
        # Get available events
        events = db.query(EventModel).all()
        
        # Initialize recommendation service with db session
        recommendation_service = RecommendationService(db)
        
        # Get personalized recommendations
        recommendations = recommendation_service.get_personalized_recommendations(events, limit=limit)
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trending", response_model=List[EventResponse])
async def get_trending_events(
    db: Session = Depends(get_db),
    timeframe_days: int = Query(7, ge=1, le=30),
    limit: int = Query(10, ge=1, le=100)
) -> List[EventModel]:
    """
    Get trending events based on popularity and recency.
    """
    try:
        # Get available events
        events = db.query(EventModel).all()
        
        # Initialize recommendation service with db session
        recommendation_service = RecommendationService(db)
        
        # Get trending recommendations
        recommendations = recommendation_service.get_trending_recommendations(
            events,
            timeframe_days=timeframe_days,
            limit=limit
        )
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/similar/{event_id}", response_model=List[EventResponse])
async def get_similar_events(
    event_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100)
) -> List[EventModel]:
    """
    Get events similar to a given event.
    """
    try:
        # Get reference event
        event = db.query(EventModel).filter(EventModel.id == event_id).first()
        if not event:
            raise HTTPException(
                status_code=404,
                detail=f"Event with id {event_id} not found"
            )
            
        # Get available events
        events = db.query(EventModel).all()
        
        # Initialize recommendation service with db session
        recommendation_service = RecommendationService(db)
        
        # Get similar recommendations
        recommendations = recommendation_service.get_similar_recommendations(
            event,
            events,
            limit=limit
        )
        return recommendations
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/category/{category}", response_model=List[EventResponse])
async def get_category_recommendations(
    category: str,
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100)
) -> List[EventModel]:
    """
    Get recommendations for a specific category.
    """
    try:
        # Get available events
        events = db.query(EventModel).all()
        
        # Get category recommendations
        recommendation_service = RecommendationService(db)
        recommendations = recommendation_service.get_category_recommendations(
            category,
            events,
            limit=limit
        )
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 