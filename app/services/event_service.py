from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from sqlalchemy import and_, or_, func, cast, Float
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import literal_column
from app.models.event import EventModel
from app.models.preferences import UserPreferences
from app.utils.pagination import PaginationParams
import logging

logger = logging.getLogger(__name__)

class EventService:
    """Service for efficient event querying and filtering."""
    
    def get_events_by_preferences(
        self,
        db: Session,
        preferences: UserPreferences,
        pagination: PaginationParams,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Tuple[List[EventModel], int]:
        """
        Get events matching user preferences with efficient filtering.
        
        Args:
            db: Database session
            preferences: User preferences
            pagination: Pagination parameters
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Tuple of (matching events, total count)
        """
        try:
            # Start with base query
            query = db.query(EventModel)
            
            # Apply date filters if provided
            if start_date:
                query = query.filter(EventModel.start_time >= start_date)
            if end_date:
                query = query.filter(EventModel.start_time <= end_date)
            
            # Apply category filters
            if preferences.preferred_categories:
                query = query.filter(
                    EventModel.categories.overlap(preferences.preferred_categories)
                )
            if preferences.excluded_categories:
                query = query.filter(
                    ~EventModel.categories.overlap(preferences.excluded_categories)
                )
            
            # Apply price filters
            if preferences.max_price < float('inf'):
                query = query.filter(
                    cast(EventModel.price_info['min_price'], Float) <= preferences.max_price
                )
            if preferences.min_price > 0:
                query = query.filter(
                    cast(EventModel.price_info['max_price'], Float) >= preferences.min_price
                )
            
            # Apply location filter if provided
            if preferences.preferred_location:
                max_distance = preferences.preferred_location.get('max_distance_km', 50)
                lat = preferences.preferred_location['lat']
                lng = preferences.preferred_location['lng']
                
                # Use PostGIS for efficient geo queries
                distance_filter = func.ST_DWithin(
                    func.ST_SetSRID(
                        func.ST_MakePoint(
                            cast(EventModel.location['lng'], Float),
                            cast(EventModel.location['lat'], Float)
                        ),
                        4326
                    ),
                    func.ST_SetSRID(
                        func.ST_MakePoint(lng, lat),
                        4326
                    ),
                    max_distance * 1000  # Convert km to meters
                )
                query = query.filter(distance_filter)
            
            # Get total count before pagination
            total_count = query.count()
            
            # Apply pagination
            query = query.offset(pagination.offset).limit(pagination.limit)
            
            # Add sorting
            if pagination.sort_by == 'start_time':
                query = query.order_by(
                    EventModel.start_time.desc() if pagination.sort_desc else EventModel.start_time
                )
            elif pagination.sort_by == 'popularity':
                query = query.order_by(
                    EventModel.popularity_score.desc() if pagination.sort_desc else EventModel.popularity_score
                )
            elif pagination.sort_by == 'price':
                price_expr = cast(EventModel.price_info['min_price'], Float)
                query = query.order_by(
                    price_expr.desc() if pagination.sort_desc else price_expr
                )
            
            return query.all(), total_count
            
        except Exception as e:
            logger.error(f"Error getting events by preferences: {str(e)}")
            return [], 0
    
    def get_trending_events(
        self,
        db: Session,
        timeframe_days: int = 7,
        pagination: PaginationParams = PaginationParams()
    ) -> Tuple[List[EventModel], int]:
        """
        Get trending events with efficient filtering and scoring.
        
        Args:
            db: Database session
            timeframe_days: Number of days to consider
            pagination: Pagination parameters
            
        Returns:
            Tuple of (trending events, total count)
        """
        try:
            now = datetime.utcnow()
            
            # Calculate trending score using window functions
            trending_score = (
                (EventModel.popularity_score * 0.7) +  # Base popularity
                (1.0 - func.extract('epoch', EventModel.start_time - now) / 
                 (timeframe_days * 24 * 3600)) * 0.3  # Time factor
            ).label('trending_score')
            
            # Build query with trending score
            query = db.query(EventModel, trending_score).filter(
                and_(
                    EventModel.start_time >= now,
                    EventModel.start_time <= now + func.make_interval(days=timeframe_days)
                )
            )
            
            # Get total count
            total_count = query.count()
            
            # Apply sorting and pagination
            query = query.order_by(trending_score.desc())
            query = query.offset(pagination.offset).limit(pagination.limit)
            
            # Extract events from results
            results = query.all()
            events = [result[0] for result in results]
            
            return events, total_count
            
        except Exception as e:
            logger.error(f"Error getting trending events: {str(e)}")
            return [], 0
    
    def get_similar_events(
        self,
        db: Session,
        event_id: int,
        limit: int = 10
    ) -> List[EventModel]:
        """
        Get similar events using efficient similarity calculation.
        
        Args:
            db: Database session
            event_id: Reference event ID
            limit: Maximum number of similar events
            
        Returns:
            List of similar events
        """
        try:
            # Get reference event
            reference = db.query(EventModel).get(event_id)
            if not reference:
                return []
            
            # Calculate similarity score using multiple factors
            category_similarity = func.array_overlap(
                EventModel.categories,
                reference.categories
            ).label('category_similarity')
            
            price_similarity = (
                1.0 - func.abs(
                    cast(EventModel.price_info['min_price'], Float) -
                    cast(literal_column(str(reference.price_info['min_price'])), Float)
                ) / 1000.0  # Normalize price difference
            ).label('price_similarity')
            
            location_similarity = (
                1.0 - func.ST_Distance(
                    func.ST_SetSRID(
                        func.ST_MakePoint(
                            cast(EventModel.location['lng'], Float),
                            cast(EventModel.location['lat'], Float)
                        ),
                        4326
                    ),
                    func.ST_SetSRID(
                        func.ST_MakePoint(
                            cast(literal_column(str(reference.location['lng'])), Float),
                            cast(literal_column(str(reference.location['lat'])), Float)
                        ),
                        4326
                    )
                ) / 10000.0  # Normalize distance
            ).label('location_similarity')
            
            # Combine similarity scores
            similarity_score = (
                category_similarity * 0.5 +
                price_similarity * 0.3 +
                location_similarity * 0.2
            ).label('similarity_score')
            
            # Query similar events
            query = db.query(EventModel, similarity_score).filter(
                EventModel.id != event_id  # Exclude reference event
            ).order_by(similarity_score.desc()).limit(limit)
            
            # Extract events from results
            results = query.all()
            return [result[0] for result in results]
            
        except Exception as e:
            logger.error(f"Error getting similar events: {str(e)}")
            return [] 