from typing import List, Optional, Dict, Any
from datetime import datetime
from app.schemas.event import Event
from app.schemas.user import UserPreferences
import json
from redis import Redis
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.database import get_db
from app.models.user import UserPreferencesModel
from app.models.event import EventModel

class StorageManager:
    """Manages storage operations for events, preferences, and cache."""
    
    def __init__(self):
        """Initialize storage manager with Redis connection."""
        self.redis = Redis(host="localhost", port=6379, db=0)
        
    async def get_user_preferences(self, user_id: str) -> Optional[UserPreferences]:
        """Get user preferences from database."""
        db = next(get_db())
        preferences = db.query(UserPreferencesModel).filter(
            UserPreferencesModel.user_id == user_id
        ).first()
        if preferences:
            return UserPreferences.from_orm(preferences)
        return None
        
    async def get_cache(self, key: str) -> Optional[List[Event]]:
        """Get cached data from Redis."""
        try:
            data = self.redis.get(key)
            if data:
                return [Event.parse_raw(event) for event in json.loads(data)]
            return None
        except Exception:
            return None
            
    async def set_cache(self, key: str, data: List[Event], expire: int = 3600) -> None:
        """Set cache data in Redis with expiration."""
        try:
            json_data = json.dumps([event.json() for event in data])
            self.redis.setex(key, expire, json_data)
        except Exception:
            pass
            
    async def get_events(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Event]:
        """Get events from database with optional date filters."""
        db = next(get_db())
        query = db.query(EventModel)
        
        if start_date:
            start = datetime.fromisoformat(start_date)
            query = query.filter(EventModel.start_datetime >= start)
            
        if end_date:
            end = datetime.fromisoformat(end_date)
            query = query.filter(EventModel.end_datetime <= end)
            
        events = query.all()
        return [Event.from_orm(event) for event in events] 