"""
Feedback system for collecting and processing user feedback on recommendations.
Includes explicit feedback (ratings, likes) and implicit feedback (clicks, views).
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func
import numpy as np
from app.database.models import User, EventModel, UserFeedback, ImplicitFeedback
from app.monitoring.ml_metrics import MLMonitor

logger = logging.getLogger(__name__)

@dataclass
class FeedbackMetrics:
    """Container for feedback metrics"""
    positive_count: int
    negative_count: int
    neutral_count: int
    total_interactions: int
    avg_rating: float
    feedback_rate: float  # % of recommendations that received feedback

class FeedbackProcessor:
    """Process and analyze user feedback for recommendations"""
    
    def __init__(self, db: Session, ml_monitor: MLMonitor):
        self.db = db
        self.ml_monitor = ml_monitor
        
    def record_explicit_feedback(
        self,
        user_id: int,
        event_id: int,
        rating: float,
        feedback_type: str,
        comment: Optional[str] = None
    ) -> UserFeedback:
        """
        Record explicit user feedback (ratings, likes, etc.)
        
        Args:
            user_id: ID of the user providing feedback
            event_id: ID of the event being rated
            rating: Numerical rating (1-5)
            feedback_type: Type of feedback (rating, like, etc.)
            comment: Optional user comment
            
        Returns:
            Created UserFeedback object
        """
        feedback = UserFeedback(
            user_id=user_id,
            event_id=event_id,
            rating=rating,
            feedback_type=feedback_type,
            comment=comment,
            timestamp=datetime.utcnow()
        )
        
        try:
            self.db.add(feedback)
            self.db.commit()
            self.db.refresh(feedback)
            
            # Record metrics
            self.ml_monitor.record_score_distribution(
                "user_feedback",
                [rating],
                processing_time=0.0,
                feature_count=1
            )
            
            logger.info(
                f"Recorded explicit feedback",
                extra={
                    'user_id': user_id,
                    'event_id': event_id,
                    'rating': rating,
                    'feedback_type': feedback_type
                }
            )
            
            return feedback
            
        except Exception as e:
            self.db.rollback()
            logger.error(
                f"Error recording feedback: {str(e)}",
                exc_info=e,
                extra={
                    'user_id': user_id,
                    'event_id': event_id
                }
            )
            raise
            
    def record_implicit_feedback(
        self,
        user_id: int,
        event_id: int,
        interaction_type: str,
        interaction_data: Optional[Dict[str, Any]] = None
    ) -> ImplicitFeedback:
        """
        Record implicit feedback (clicks, views, time spent)
        
        Args:
            user_id: ID of the user
            event_id: ID of the event
            interaction_type: Type of interaction (click, view, etc.)
            interaction_data: Additional interaction data
            
        Returns:
            Created ImplicitFeedback object
        """
        feedback = ImplicitFeedback(
            user_id=user_id,
            event_id=event_id,
            interaction_type=interaction_type,
            interaction_data=interaction_data or {},
            timestamp=datetime.utcnow()
        )
        
        try:
            self.db.add(feedback)
            self.db.commit()
            self.db.refresh(feedback)
            
            logger.info(
                f"Recorded implicit feedback",
                extra={
                    'user_id': user_id,
                    'event_id': event_id,
                    'interaction_type': interaction_type
                }
            )
            
            return feedback
            
        except Exception as e:
            self.db.rollback()
            logger.error(
                f"Error recording implicit feedback: {str(e)}",
                exc_info=e,
                extra={
                    'user_id': user_id,
                    'event_id': event_id
                }
            )
            raise
            
    def get_user_feedback_metrics(
        self,
        user_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> FeedbackMetrics:
        """
        Get feedback metrics for a specific user
        
        Args:
            user_id: ID of the user
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            FeedbackMetrics object with user's feedback statistics
        """
        query = self.db.query(UserFeedback).filter(UserFeedback.user_id == user_id)
        
        if start_date:
            query = query.filter(UserFeedback.timestamp >= start_date)
        if end_date:
            query = query.filter(UserFeedback.timestamp <= end_date)
            
        feedback_list = query.all()
        
        if not feedback_list:
            return FeedbackMetrics(
                positive_count=0,
                negative_count=0,
                neutral_count=0,
                total_interactions=0,
                avg_rating=0.0,
                feedback_rate=0.0
            )
            
        # Calculate metrics
        ratings = [f.rating for f in feedback_list if f.rating is not None]
        
        positive_count = sum(1 for r in ratings if r >= 4.0)
        negative_count = sum(1 for r in ratings if r <= 2.0)
        neutral_count = sum(1 for r in ratings if 2.0 < r < 4.0)
        
        # Get total recommendations for feedback rate calculation
        total_recommendations = self.db.query(EventModel).join(
            ImplicitFeedback,
            ImplicitFeedback.event_id == EventModel.id
        ).filter(
            ImplicitFeedback.user_id == user_id,
            ImplicitFeedback.interaction_type == 'recommendation'
        ).count()
        
        return FeedbackMetrics(
            positive_count=positive_count,
            negative_count=negative_count,
            neutral_count=neutral_count,
            total_interactions=len(feedback_list),
            avg_rating=sum(ratings) / len(ratings) if ratings else 0.0,
            feedback_rate=len(feedback_list) / total_recommendations
            if total_recommendations > 0 else 0.0
        )
        
    def get_event_feedback_metrics(
        self,
        event_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> FeedbackMetrics:
        """
        Get feedback metrics for a specific event
        
        Args:
            event_id: ID of the event
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            FeedbackMetrics object with event's feedback statistics
        """
        query = self.db.query(UserFeedback).filter(UserFeedback.event_id == event_id)
        
        if start_date:
            query = query.filter(UserFeedback.timestamp >= start_date)
        if end_date:
            query = query.filter(UserFeedback.timestamp <= end_date)
            
        feedback_list = query.all()
        
        if not feedback_list:
            return FeedbackMetrics(
                positive_count=0,
                negative_count=0,
                neutral_count=0,
                total_interactions=0,
                avg_rating=0.0,
                feedback_rate=0.0
            )
            
        # Calculate metrics
        ratings = [f.rating for f in feedback_list if f.rating is not None]
        
        positive_count = sum(1 for r in ratings if r >= 4.0)
        negative_count = sum(1 for r in ratings if r <= 2.0)
        neutral_count = sum(1 for r in ratings if 2.0 < r < 4.0)
        
        # Get total views for feedback rate calculation
        total_views = self.db.query(ImplicitFeedback).filter(
            ImplicitFeedback.event_id == event_id,
            ImplicitFeedback.interaction_type == 'view'
        ).count()
        
        return FeedbackMetrics(
            positive_count=positive_count,
            negative_count=negative_count,
            neutral_count=neutral_count,
            total_interactions=len(feedback_list),
            avg_rating=sum(ratings) / len(ratings) if ratings else 0.0,
            feedback_rate=len(feedback_list) / total_views if total_views > 0 else 0.0
        )
        
    def adjust_recommendation_weights(
        self,
        user_id: int,
        feedback_window_days: int = 30
    ) -> Dict[str, float]:
        """
        Adjust recommendation weights based on user feedback
        
        Args:
            user_id: ID of the user
            feedback_window_days: Days of feedback to consider
            
        Returns:
            Dictionary of adjusted weights for different recommendation factors
        """
        start_date = datetime.utcnow() - timedelta(days=feedback_window_days)
        
        # Get user's feedback history
        metrics = self.get_user_feedback_metrics(
            user_id,
            start_date=start_date
        )
        
        # Base weights
        weights = {
            'content_similarity': 1.0,
            'popularity': 1.0,
            'recency': 1.0,
            'social_proof': 1.0
        }
        
        if metrics.total_interactions == 0:
            return weights
            
        # Adjust weights based on feedback patterns
        positive_ratio = metrics.positive_count / metrics.total_interactions
        
        # Increase content similarity weight for users with clear preferences
        if positive_ratio > 0.7:
            weights['content_similarity'] *= 1.5
            weights['popularity'] *= 0.8
            
        # Increase popularity weight for users with mixed feedback
        elif 0.3 <= positive_ratio <= 0.7:
            weights['popularity'] *= 1.3
            weights['social_proof'] *= 1.2
            
        # Increase recency weight for users with negative feedback
        else:
            weights['recency'] *= 1.5
            weights['content_similarity'] *= 0.8
            
        logger.info(
            f"Adjusted recommendation weights for user {user_id}",
            extra={
                'user_id': user_id,
                'weights': weights,
                'positive_ratio': positive_ratio
            }
        )
        
        return weights 