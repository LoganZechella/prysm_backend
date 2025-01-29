import pytest
from datetime import datetime, timedelta
import numpy as np
from app.monitoring.ml_metrics import MLMonitor, MLMetrics

class TestMLMonitoring:
    @pytest.fixture
    def ml_monitor(self):
        return MLMonitor(window_minutes=60)
        
    def test_score_distribution_recording(self, ml_monitor):
        """Test recording and retrieving score distributions"""
        scores = [0.5, 0.7, 0.3, 0.8, 0.6]
        ml_monitor.record_score_distribution(
            "test_model",
            scores,
            processing_time=1.2,
            feature_count=10
        )
        
        metrics = ml_monitor.get_performance_metrics("test_model")
        assert abs(metrics['mean_score'] - np.mean(scores)) < 1e-6
        assert abs(metrics['std_score'] - np.std(scores)) < 1e-6
        assert metrics['avg_processing_time'] == 1.2
        assert metrics['feature_count'] == 10
        assert metrics['sample_size'] == 1
        
    def test_feature_extraction_tracking(self, ml_monitor):
        """Test tracking feature extraction success/failure"""
        # Record some successes and failures
        ml_monitor.record_feature_extraction("text_features", True)
        ml_monitor.record_feature_extraction("text_features", True)
        ml_monitor.record_feature_extraction("text_features", False, Exception("Test error"))
        
        stats = ml_monitor.get_feature_extraction_stats("text_features")
        assert stats["text_features"]["success_count"] == 2
        assert stats["text_features"]["failure_count"] == 1
        assert abs(stats["text_features"]["success_rate"] - 0.667) < 0.01
        
    def test_performance_degradation_detection(self, ml_monitor):
        """Test detection of model performance degradation"""
        # Record baseline performance
        ml_monitor.record_score_distribution(
            "test_model",
            [0.8, 0.9, 0.85, 0.87],
            processing_time=1.0,
            feature_count=10
        )
        
        # Record degraded performance
        ml_monitor.record_score_distribution(
            "test_model",
            [0.6, 0.65, 0.62, 0.63],  # Lower scores
            processing_time=2.0,  # Higher processing time
            feature_count=10,
            error_count=2  # More errors
        )
        
        metrics = ml_monitor.get_performance_metrics("test_model")
        assert metrics['mean_score'] < 0.8  # Should detect lower scores
        assert metrics['avg_processing_time'] > 1.5  # Should detect slower processing
        assert metrics['error_rate'] > 0  # Should detect errors
        
    def test_time_window_filtering(self, ml_monitor):
        """Test filtering of metrics by time window"""
        # Record old metrics (outside window)
        old_scores = [0.5, 0.6, 0.55]
        ml_monitor.metrics["test_model"].append(
            MLMetrics(
                mean_score=np.mean(old_scores),
                std_score=np.std(old_scores),
                min_score=min(old_scores),
                max_score=max(old_scores),
                processing_time=1.0,
                feature_count=10,
                error_rate=0.0,
                timestamp=datetime.utcnow() - timedelta(minutes=120)
            )
        )
        
        # Record recent metrics
        recent_scores = [0.8, 0.9, 0.85]
        ml_monitor.record_score_distribution(
            "test_model",
            recent_scores,
            processing_time=1.0,
            feature_count=10
        )
        
        # Get metrics with default window (60 minutes)
        metrics = ml_monitor.get_performance_metrics("test_model")
        assert abs(metrics['mean_score'] - np.mean(recent_scores)) < 1e-6
        assert metrics['sample_size'] == 1  # Should only include recent metrics
        
    def test_multiple_component_tracking(self, ml_monitor):
        """Test tracking metrics for multiple components"""
        # Record metrics for two different components
        ml_monitor.record_score_distribution(
            "model_a",
            [0.8, 0.9, 0.85],
            processing_time=1.0,
            feature_count=10
        )
        
        ml_monitor.record_score_distribution(
            "model_b",
            [0.6, 0.7, 0.65],
            processing_time=2.0,
            feature_count=15
        )
        
        metrics_a = ml_monitor.get_performance_metrics("model_a")
        metrics_b = ml_monitor.get_performance_metrics("model_b")
        
        assert metrics_a['mean_score'] > metrics_b['mean_score']
        assert metrics_a['processing_time'] < metrics_b['processing_time']
        assert metrics_a['feature_count'] < metrics_b['feature_count']
        
    def test_error_handling(self, ml_monitor):
        """Test handling of edge cases and errors"""
        # Test empty scores list
        ml_monitor.record_score_distribution(
            "test_model",
            [],
            processing_time=1.0,
            feature_count=10
        )
        
        metrics = ml_monitor.get_performance_metrics("test_model")
        assert metrics['mean_score'] == 0.0
        assert metrics['sample_size'] == 0
        
        # Test non-existent component
        metrics = ml_monitor.get_performance_metrics("non_existent")
        assert metrics['mean_score'] == 0.0
        assert metrics['sample_size'] == 0
        
        # Test feature extraction error tracking
        ml_monitor.record_feature_extraction(
            "test_features",
            False,
            Exception("Test error")
        )
        
        stats = ml_monitor.get_feature_extraction_stats("test_features")
        assert stats["test_features"]["failure_count"] == 1
        assert stats["test_features"]["success_rate"] == 0.0 