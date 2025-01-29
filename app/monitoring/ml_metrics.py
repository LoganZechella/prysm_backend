"""
ML Pipeline monitoring system for tracking model performance, feature extraction,
and recommendation quality metrics.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from collections import defaultdict
import numpy as np
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class MLMetrics:
    """Container for ML performance metrics"""
    mean_score: float
    std_score: float
    min_score: float
    max_score: float
    processing_time: float
    feature_count: int
    error_rate: float
    timestamp: datetime

class MLMonitor:
    """Monitor ML pipeline performance and health"""
    
    def __init__(
        self,
        window_minutes: int = 60,
        score_threshold: float = 0.1,
        processing_time_threshold: float = 5.0
    ):
        self.window_minutes = window_minutes
        self.score_threshold = score_threshold
        self.processing_time_threshold = processing_time_threshold
        
        # Store metrics by model/component
        self.metrics: Dict[str, List[MLMetrics]] = defaultdict(list)
        
        # Track feature extraction success/failure
        self.feature_extraction_stats: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {'success': 0, 'failure': 0}
        )
        
        # Track processing times
        self.processing_times: Dict[str, List[float]] = defaultdict(list)
        
        # Track model performance degradation
        self.baseline_metrics: Dict[str, MLMetrics] = {}
        
    def record_score_distribution(
        self,
        component_name: str,
        scores: List[float],
        processing_time: float,
        feature_count: int,
        error_count: int = 0
    ) -> None:
        """
        Record score distribution metrics for a component.
        
        Args:
            component_name: Name of the ML component
            scores: List of scores generated
            processing_time: Time taken to generate scores
            feature_count: Number of features used
            error_count: Number of errors encountered
        """
        if not scores:
            logger.warning(f"No scores to record for {component_name}")
            return
            
        metrics = MLMetrics(
            mean_score=float(np.mean(scores)),
            std_score=float(np.std(scores)),
            min_score=float(np.min(scores)),
            max_score=float(np.max(scores)),
            processing_time=processing_time,
            feature_count=feature_count,
            error_rate=error_count / len(scores) if scores else 0,
            timestamp=datetime.utcnow()
        )
        
        self.metrics[component_name].append(metrics)
        self.processing_times[component_name].append(processing_time)
        
        # Check for performance degradation
        self._check_performance_degradation(component_name, metrics)
        
        # Trim old data
        self._trim_old_data(component_name)
        
        # Log metrics
        logger.info(
            f"ML Metrics recorded for {component_name}",
            extra={
                'component': component_name,
                'mean_score': metrics.mean_score,
                'std_score': metrics.std_score,
                'processing_time': metrics.processing_time,
                'feature_count': metrics.feature_count,
                'error_rate': metrics.error_rate
            }
        )
        
    def record_feature_extraction(
        self,
        feature_type: str,
        success: bool,
        error: Optional[Exception] = None
    ) -> None:
        """
        Record feature extraction success/failure.
        
        Args:
            feature_type: Type of feature being extracted
            success: Whether extraction succeeded
            error: Optional exception if extraction failed
        """
        if success:
            self.feature_extraction_stats[feature_type]['success'] += 1
        else:
            self.feature_extraction_stats[feature_type]['failure'] += 1
            if error:
                logger.error(
                    f"Feature extraction error for {feature_type}: {str(error)}",
                    exc_info=error
                )
                
    def get_performance_metrics(
        self,
        component_name: str,
        window_minutes: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get performance metrics for a component.
        
        Args:
            component_name: Name of the ML component
            window_minutes: Optional custom time window
            
        Returns:
            Dictionary of performance metrics
        """
        window = window_minutes or self.window_minutes
        cutoff = datetime.utcnow() - timedelta(minutes=window)
        
        # Filter metrics within window
        recent_metrics = [
            m for m in self.metrics[component_name]
            if m.timestamp > cutoff
        ]
        
        if not recent_metrics:
            return {
                'mean_score': 0.0,
                'std_score': 0.0,
                'avg_processing_time': 0.0,
                'error_rate': 0.0,
                'feature_count': 0,
                'sample_size': 0
            }
            
        return {
            'mean_score': np.mean([m.mean_score for m in recent_metrics]),
            'std_score': np.mean([m.std_score for m in recent_metrics]),
            'avg_processing_time': np.mean([m.processing_time for m in recent_metrics]),
            'error_rate': np.mean([m.error_rate for m in recent_metrics]),
            'feature_count': int(np.mean([m.feature_count for m in recent_metrics])),
            'sample_size': len(recent_metrics)
        }
        
    def get_feature_extraction_stats(
        self,
        feature_type: Optional[str] = None
    ) -> Dict[str, Dict[str, Union[int, float]]]:
        """
        Get feature extraction success rates.
        
        Args:
            feature_type: Optional specific feature type to get stats for
            
        Returns:
            Dictionary of feature extraction statistics
        """
        if feature_type:
            stats = self.feature_extraction_stats[feature_type]
            total = stats['success'] + stats['failure']
            return {
                feature_type: {
                    'success_count': stats['success'],
                    'failure_count': stats['failure'],
                    'success_rate': stats['success'] / total if total > 0 else 0
                }
            }
            
        results = {}
        for feat_type, stats in self.feature_extraction_stats.items():
            total = stats['success'] + stats['failure']
            results[feat_type] = {
                'success_count': stats['success'],
                'failure_count': stats['failure'],
                'success_rate': stats['success'] / total if total > 0 else 0
            }
        return results
        
    def _check_performance_degradation(
        self,
        component_name: str,
        current_metrics: MLMetrics
    ) -> None:
        """Check for model performance degradation"""
        if component_name not in self.baseline_metrics:
            self.baseline_metrics[component_name] = current_metrics
            return
            
        baseline = self.baseline_metrics[component_name]
        
        # Check for significant degradation
        if (
            current_metrics.mean_score < baseline.mean_score - self.score_threshold
            or current_metrics.processing_time > baseline.processing_time * 1.5
            or current_metrics.error_rate > baseline.error_rate * 1.5
        ):
            logger.warning(
                f"Performance degradation detected for {component_name}",
                extra={
                    'component': component_name,
                    'baseline_score': baseline.mean_score,
                    'current_score': current_metrics.mean_score,
                    'baseline_time': baseline.processing_time,
                    'current_time': current_metrics.processing_time,
                    'baseline_error_rate': baseline.error_rate,
                    'current_error_rate': current_metrics.error_rate
                }
            )
            
    def _trim_old_data(self, component_name: str) -> None:
        """Remove metrics outside the time window"""
        cutoff = datetime.utcnow() - timedelta(minutes=self.window_minutes)
        self.metrics[component_name] = [
            m for m in self.metrics[component_name]
            if m.timestamp > cutoff
        ] 