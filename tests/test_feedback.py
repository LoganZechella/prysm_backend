import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from app.recommendation.feedback import FeedbackProcessor, FeedbackMetrics
from app.database.models import User, EventModel, UserFeedback, ImplicitFeedback
from app.monitoring.ml_metrics import MLMonitor

class TestFeedbackSystem:
    @pytest.fixture
    def db_session(self):
        """Mock database session"""
        session = Mock()
        session.query = Mock(return_value=session)
        session.filter = Mock(return_value=session)
        session.all = Mock(return_value=[])
        return session
        
    @pytest.fixture
    def ml_monitor(self):
        """Mock ML monitor"""
        return Mock(spec=MLMonitor)
        
    @pytest.fixture
    def feedback_processor(self, db_session, ml_monitor):
        return FeedbackProcessor(db_session, ml_monitor)
        
    def test_record_explicit_feedback(self, feedback_processor, db_session):
        """Test recording explicit feedback"""
        # Setup
        user_id = 1
        event_id = 1
        rating = 4.5
        feedback_type = "rating"
        comment = "Great event!"
        
        # Execute
        feedback = feedback_processor.record_explicit_feedback(
            user_id=user_id,
            event_id=event_id,
            rating=rating,
            feedback_type=feedback_type,
            comment=comment
        )
        
        # Verify
        assert db_session.add.called
        assert db_session.commit.called
        assert feedback_processor.ml_monitor.record_score_distribution.called
        
        # Verify feedback object
        assert feedback.user_id == user_id
        assert feedback.event_id == event_id
        assert feedback.rating == rating
        assert feedback.feedback_type == feedback_type
        assert feedback.comment == comment
        
    def test_record_implicit_feedback(self, feedback_processor, db_session):
        """Test recording implicit feedback"""
        # Setup
        user_id = 1
        event_id = 1
        interaction_type = "view"
        interaction_data = {"duration": 300}  # 5 minutes
        
        # Execute
        feedback = feedback_processor.record_implicit_feedback(
            user_id=user_id,
            event_id=event_id,
            interaction_type=interaction_type,
            interaction_data=interaction_data
        )
        
        # Verify
        assert db_session.add.called
        assert db_session.commit.called
        
        # Verify feedback object
        assert feedback.user_id == user_id
        assert feedback.event_id == event_id
        assert feedback.interaction_type == interaction_type
        assert feedback.interaction_data == interaction_data
        
    def test_get_user_feedback_metrics(self, feedback_processor, db_session):
        """Test getting user feedback metrics"""
        # Setup mock feedback data
        feedback_list = [
            Mock(rating=5.0),
            Mock(rating=4.0),
            Mock(rating=3.0),
            Mock(rating=2.0),
            Mock(rating=1.0)
        ]
        db_session.all.return_value = feedback_list
        db_session.count.return_value = 10  # Total recommendations
        
        # Execute
        metrics = feedback_processor.get_user_feedback_metrics(
            user_id=1,
            start_date=datetime.utcnow() - timedelta(days=30)
        )
        
        # Verify
        assert isinstance(metrics, FeedbackMetrics)
        assert metrics.positive_count == 2  # ratings >= 4.0
        assert metrics.negative_count == 2  # ratings <= 2.0
        assert metrics.neutral_count == 1   # 2.0 < ratings < 4.0
        assert metrics.total_interactions == 5
        assert metrics.avg_rating == 3.0
        assert metrics.feedback_rate == 0.5  # 5 feedbacks / 10 recommendations
        
    def test_get_event_feedback_metrics(self, feedback_processor, db_session):
        """Test getting event feedback metrics"""
        # Setup mock feedback data
        feedback_list = [
            Mock(rating=5.0),
            Mock(rating=5.0),
            Mock(rating=4.0)
        ]
        db_session.all.return_value = feedback_list
        db_session.count.return_value = 6  # Total views
        
        # Execute
        metrics = feedback_processor.get_event_feedback_metrics(
            event_id=1,
            start_date=datetime.utcnow() - timedelta(days=7)
        )
        
        # Verify
        assert isinstance(metrics, FeedbackMetrics)
        assert metrics.positive_count == 3  # All ratings >= 4.0
        assert metrics.negative_count == 0
        assert metrics.neutral_count == 0
        assert metrics.total_interactions == 3
        assert abs(metrics.avg_rating - 4.67) < 0.01
        assert metrics.feedback_rate == 0.5  # 3 feedbacks / 6 views
        
    def test_adjust_recommendation_weights(self, feedback_processor, db_session):
        """Test recommendation weight adjustment"""
        # Setup mock feedback metrics
        feedback_list = [Mock(rating=4.5), Mock(rating=4.8), Mock(rating=4.7)]
        db_session.all.return_value = feedback_list
        db_session.count.return_value = 3
        
        # Execute
        weights = feedback_processor.adjust_recommendation_weights(
            user_id=1,
            feedback_window_days=30
        )
        
        # Verify
        assert isinstance(weights, dict)
        assert all(k in weights for k in [
            'content_similarity',
            'popularity',
            'recency',
            'social_proof'
        ])
        
        # All ratings are high, so content similarity should be weighted higher
        assert weights['content_similarity'] > weights['popularity']
        
    def test_error_handling(self, feedback_processor, db_session):
        """Test error handling in feedback recording"""
        # Setup database error
        db_session.commit.side_effect = Exception("Database error")
        
        # Test explicit feedback error handling
        with pytest.raises(Exception):
            feedback_processor.record_explicit_feedback(
                user_id=1,
                event_id=1,
                rating=4.0,
                feedback_type="rating"
            )
        assert db_session.rollback.called
        
        # Reset mock
        db_session.reset_mock()
        
        # Test implicit feedback error handling
        with pytest.raises(Exception):
            feedback_processor.record_implicit_feedback(
                user_id=1,
                event_id=1,
                interaction_type="view"
            )
        assert db_session.rollback.called
        
    def test_empty_feedback_handling(self, feedback_processor, db_session):
        """Test handling of empty feedback data"""
        # Setup empty feedback
        db_session.all.return_value = []
        db_session.count.return_value = 0
        
        # Test user metrics
        user_metrics = feedback_processor.get_user_feedback_metrics(user_id=1)
        assert user_metrics.total_interactions == 0
        assert user_metrics.avg_rating == 0.0
        assert user_metrics.feedback_rate == 0.0
        
        # Test event metrics
        event_metrics = feedback_processor.get_event_feedback_metrics(event_id=1)
        assert event_metrics.total_interactions == 0
        assert event_metrics.avg_rating == 0.0
        assert event_metrics.feedback_rate == 0.0
        
        # Test weight adjustment with no feedback
        weights = feedback_processor.adjust_recommendation_weights(user_id=1)
        assert weights['content_similarity'] == 1.0  # Should return base weights
        assert weights['popularity'] == 1.0
        assert weights['recency'] == 1.0
        assert weights['social_proof'] == 1.0 