from fastapi import APIRouter, HTTPException, Request, Depends, Query
from typing import List, Dict, Any, cast, Optional
from app.utils.auth import verify_session
from app.utils.recommendation import RecommendationEngine
from app.utils.schema import Event, UserPreferences as SchemaUserPreferences, Location, PriceInfo, SourceInfo
from app.models.preferences import UserPreferences as DBUserPreferences
from app.database import get_db
from app.utils.storage import StorageManager
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import logging
import traceback
from cachetools import TTLCache, cached
from functools import lru_cache
import hashlib
from unittest.mock import MagicMock
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter()
recommendation_engine = RecommendationEngine()

# Initialize storage manager with a mock client in test environment
if "pytest" in sys.modules:
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.get_bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    mock_blob.download_as_string.return_value = b'{"test": "data"}'
    storage_manager = StorageManager(client=mock_client)
else:
    storage_manager = StorageManager()

# Cache for events (1 hour TTL)
events_cache = TTLCache(maxsize=100, ttl=3600)
# Cache for recommendations (15 minutes TTL)
recommendations_cache = TTLCache(maxsize=1000, ttl=900)

# Default coordinates for major cities
DEFAULT_COORDINATES = {
    "san francisco": {"lat": 37.7749, "lng": -122.4194},
    "chicago": {"lat": 41.8781, "lng": -87.6298},
    "new york": {"lat": 40.7128, "lng": -74.0060},
}

def generate_cache_key(user_id: str, start_date: datetime, end_date: datetime, page: int, page_size: int) -> str:
    """Generate a cache key for recommendations"""
    key_parts = [
        user_id,
        start_date.isoformat(),
        end_date.isoformat(),
        str(page),
        str(page_size)
    ]
    return hashlib.md5("|".join(key_parts).encode()).hexdigest()

async def get_available_events(start_date: datetime, end_date: datetime) -> List[Event]:
    """Fetch available events from storage within the given date range."""
    cache_key = f"events_{start_date.isoformat()}_{end_date.isoformat()}"
    
    # Check cache first
    if cache_key in events_cache:
        logger.debug("Returning events from cache")
        return events_cache[cache_key]
    
    try:
        events = []
        current_date = start_date
        
        # Fetch events month by month
        while current_date <= end_date:
            logger.debug(f"Fetching events for {current_date.year}/{current_date.month}")
            blob_names = await storage_manager.list_processed_events(
                year=current_date.year,
                month=current_date.month
            )
            
            # Fetch each event's data
            for blob_name in blob_names:
                try:
                    event_data = await storage_manager.get_processed_event(blob_name)
                    if event_data:
                        event = Event.from_raw_data(event_data)
                        # Only include events within the date range
                        if start_date <= event.start_datetime <= end_date:
                            events.append(event)
                except Exception as e:
                    logger.error(f"Error processing event {blob_name}: {str(e)}")
                    continue
            
            # Move to next month
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        logger.debug(f"Found {len(events)} events between {start_date} and {end_date}")
        
        # Cache the results
        events_cache[cache_key] = events
        return events
        
    except Exception as e:
        logger.error(f"Error fetching events: {str(e)}")
        logger.error(traceback.format_exc())
        return []

@router.get("")
async def get_recommendations(
    request: Request,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of recommendations per page"),
    days_ahead: int = Query(30, ge=1, le=365, description="Number of days to look ahead"),
    db: Session = Depends(get_db)
):
    """Get personalized event recommendations for the current user."""
    try:
        # Verify session and get user ID
        session = await verify_session(request)
        user_id = session.get_user_id()
        logger.debug(f"Got user_id: {user_id}")
        
        # Calculate date range
        start_date = datetime.now()
        end_date = start_date + timedelta(days=days_ahead)
        
        # Generate cache key
        cache_key = generate_cache_key(user_id, start_date, end_date, page, page_size)
        
        # Check cache
        if cache_key in recommendations_cache:
            logger.debug("Returning recommendations from cache")
            return recommendations_cache[cache_key]
        
        # Get user preferences from database
        db_preferences = db.query(DBUserPreferences).filter_by(user_id=user_id).first()
        
        if not db_preferences:
            logger.warning(f"No preferences found for user {user_id}, using defaults")
            db_preferences = DBUserPreferences(user_id=user_id)
            db.add(db_preferences)
            db.commit()
            db.refresh(db_preferences)
        
        # Convert DB model to schema model using to_dict()
        prefs_dict = db_preferences.to_dict()
        logger.debug(f"User preferences: {prefs_dict}")
        
        # Convert location to coordinates
        location_dict = prefs_dict.get("preferred_location", {})
        logger.debug(f"Location dict: {location_dict}")
        city = location_dict.get("city", "").lower() if location_dict else ""
        coordinates = DEFAULT_COORDINATES.get(city, {"lat": 37.7749, "lng": -122.4194})  # Default to SF
        logger.debug(f"Using coordinates: {coordinates}")
        
        user_preferences = SchemaUserPreferences(
            user_id=prefs_dict["user_id"],
            preferred_categories=prefs_dict["preferred_categories"],
            price_preferences=["free", "budget", "medium"],  # TODO: Add to preferences model
            preferred_location=coordinates,
            preferred_radius=50.0,  # TODO: Add to preferences model
            min_price=prefs_dict["min_price"],
            max_price=prefs_dict["max_price"]
        )
        logger.debug(f"Created schema user preferences: {user_preferences.model_dump_json()}")
        
        # Get available events
        available_events = await get_available_events(start_date, end_date)
        logger.debug(f"Fetched {len(available_events)} available events")
        
        if not available_events:
            logger.warning("No events found in the date range")
            empty_response = {
                "recommendations": {
                    "events": [],
                    "network": []
                },
                "user_id": user_id,
                "timestamp": datetime.now().isoformat(),
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_items": 0,
                    "total_pages": 0
                }
            }
            recommendations_cache[cache_key] = empty_response
            return empty_response
        
        # Get personalized recommendations
        all_recommendations = recommendation_engine.get_personalized_recommendations(
            user_preferences=user_preferences,
            available_events=available_events
        )
        logger.debug(f"Got {len(all_recommendations)} recommendations")
        
        # Apply pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_recommendations = all_recommendations[start_idx:end_idx]
        
        # Format recommendations for frontend
        formatted_recommendations = {
            "recommendations": {
                "events": [
                    {
                        "event_id": event.event_id,
                        "title": event.title,
                        "description": event.description,
                        "start_datetime": event.start_datetime.isoformat(),
                        "location": {
                            "venue_name": event.location.venue_name,
                            "city": event.location.city,
                            "coordinates": event.location.coordinates
                        },
                        "categories": event.categories,
                        "price_info": {
                            "min_price": event.price_info.min_price,
                            "max_price": event.price_info.max_price,
                            "price_tier": event.price_info.price_tier
                        },
                        "images": event.images[:1] if event.images else [],  # Include first image if available
                        "source": {
                            "platform": event.source.platform,
                            "url": event.source.url
                        },
                        "match_score": score
                    }
                    for event, score in paginated_recommendations
                ],
                "network": []  # TODO: Add network recommendations
            },
            "user_id": user_id,
            "timestamp": datetime.now().isoformat(),
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_items": len(all_recommendations),
                "total_pages": (len(all_recommendations) + page_size - 1) // page_size
            }
        }
        logger.debug("Successfully formatted recommendations")
        
        # Cache the response
        recommendations_cache[cache_key] = formatted_recommendations
        return formatted_recommendations
            
    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get recommendations: {str(e)}"
        ) 