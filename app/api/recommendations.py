from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from app.services.recommendation import RecommendationService
from app.schemas import Event, UserPreferences
from app.storage import StorageManager
from app.auth import get_current_user

router = APIRouter()
recommendation_service = RecommendationService()
storage_manager = StorageManager()

@router.get("/recommendations", response_model=List[Event])
async def get_recommendations(
    user: str = Depends(get_current_user),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> List[Event]:
    """
    Get personalized event recommendations for the current user.
    
    Args:
        user: Current authenticated user
        page: Page number for pagination
        page_size: Number of items per page
        start_date: Optional start date filter (ISO format)
        end_date: Optional end date filter (ISO format)
    
    Returns:
        List of recommended events
    """
    # Get user preferences
    preferences = await storage_manager.get_user_preferences(user)
    if not preferences:
        preferences = UserPreferences(user_id=user)
    
    # Generate cache key
    cache_key = f"recommendations:{user}:{page}:{page_size}"
    if start_date:
        cache_key += f":{start_date}"
    if end_date:
        cache_key += f":{end_date}"
    
    # Try to get cached recommendations
    cached_recommendations = await storage_manager.get_cache(cache_key)
    if cached_recommendations:
        return cached_recommendations
    
    # Get available events
    events = await storage_manager.get_events(start_date, end_date)
    
    # Generate recommendations
    recommendations = recommendation_service.get_personalized_recommendations(
        preferences=preferences,
        events=events,
        limit=page_size
    )
    
    # Cache recommendations
    await storage_manager.set_cache(cache_key, recommendations)
    
    return recommendations 