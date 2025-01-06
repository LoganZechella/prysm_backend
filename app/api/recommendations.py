from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from datetime import datetime, timedelta
from app.services.recommendation import RecommendationService
from app.models.event import Event
from app.models.preferences import UserPreferences
from app.schemas.event import EventResponse
from app.database import get_db
from sqlalchemy.orm import Session
from app.auth import get_current_user
import logging

logger = logging.getLogger(__name__)
router = APIRouter()
recommendation_service = RecommendationService()

def create_sample_events(db: Session) -> None:
    """Create sample events if none exist."""
    if db.query(Event).count() == 0:
        sample_events = [
            Event(
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
            Event(
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
            Event(
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
        events = db.query(Event).all()
        
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
    user_id: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Event]:
    """
    Get personalized event recommendations for the current user.
    
    Args:
        user_id: Current authenticated user
        page: Page number for pagination
        page_size: Number of items per page
        start_date: Optional start date filter
        end_date: Optional end date filter
    
    Returns:
        List of recommended events
    """
    try:
        # Get user preferences
        preferences = db.query(UserPreferences).filter(
            UserPreferences.user_id == user_id
        ).first()
        
        if not preferences:
            preferences = UserPreferences(user_id=user_id)
            
        # Get available events
        events = db.query(Event).all()
        
        # Generate recommendations
        recommendations = await recommendation_service.get_personalized_recommendations(
            preferences=preferences,
            events=events,
            limit=page_size,
            start_date=start_date,
            end_date=end_date
        )
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Error getting personalized recommendations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get recommendations"
        )

@router.get("/trending", response_model=List[EventResponse])
async def get_trending_recommendations(
    db: Session = Depends(get_db),
    timeframe_days: int = Query(7, ge=1, le=30),
    limit: int = Query(10, ge=1, le=100)
) -> List[Event]:
    """
    Get trending events based on popularity and recency.
    
    Args:
        timeframe_days: Number of days to consider for trending
        limit: Maximum number of recommendations to return
    
    Returns:
        List of trending events
    """
    try:
        # Get available events
        events = db.query(Event).all()
        
        # Get trending recommendations
        trending = await recommendation_service.get_trending_recommendations(
            events=events,
            timeframe_days=timeframe_days,
            limit=limit
        )
        
        return trending
        
    except Exception as e:
        logger.error(f"Error getting trending recommendations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get trending events"
        )

@router.get("/similar/{event_id}", response_model=List[EventResponse])
async def get_similar_events(
    event_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100)
) -> List[Event]:
    """
    Get events similar to a given event.
    
    Args:
        event_id: ID of the reference event
        limit: Maximum number of similar events to return
    
    Returns:
        List of similar events
    """
    try:
        # Get reference event
        event = db.query(Event).filter(Event.id == event_id).first()
        if not event:
            raise HTTPException(
                status_code=404,
                detail="Event not found"
            )
            
        # Get available events
        events = db.query(Event).all()
        
        # Get similar events
        similar = await recommendation_service.get_similar_events(
            event=event,
            events=events,
            limit=limit
        )
        
        return similar
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting similar events: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get similar events"
        )

@router.get("/category/{category}", response_model=List[EventResponse])
async def get_category_recommendations(
    category: str,
    db: Session = Depends(get_db),
    limit: int = Query(10, ge=1, le=100)
) -> List[Event]:
    """
    Get recommendations for a specific category.
    
    Args:
        category: Target category
        limit: Maximum number of recommendations to return
    
    Returns:
        List of recommended events in the category
    """
    try:
        # Get available events
        events = db.query(Event).all()
        
        # Get category recommendations
        category_events = await recommendation_service.get_category_recommendations(
            category=category,
            events=events,
            limit=limit
        )
        
        return category_events
        
    except Exception as e:
        logger.error(f"Error getting category recommendations: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get category recommendations"
        ) 