"""Base class for feature processors."""
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import numpy as np
from datetime import datetime
import logging

from app.monitoring.performance import PerformanceMonitor

logger = logging.getLogger(__name__)

class BaseFeatureProcessor(ABC):
    """Base class for all feature processors in the recommendation engine."""
    
    def __init__(self):
        """Initialize the feature processor."""
        self.performance_monitor = PerformanceMonitor()
        self._feature_names: List[str] = []
        self._feature_importance: Dict[str, float] = {}
        self.last_processed: datetime | None = None
        
    @property
    def feature_names(self) -> List[str]:
        """Get the names of features this processor generates."""
        return self._feature_names
        
    @property
    def feature_importance(self) -> Dict[str, float]:
        """Get the importance scores for each feature."""
        return self._feature_importance
    
    def _validate_features(self, features: Dict[str, Any]) -> bool:
        """Validate that all required features are present."""
        return all(name in features for name in self.feature_names)
    
    def _normalize_vector(self, vector: np.ndarray) -> np.ndarray:
        """Normalize a feature vector to unit length."""
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector
        return vector / norm
    
    @abstractmethod
    async def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw data into feature vectors.
        
        Args:
            data: Raw data to process
            
        Returns:
            Dictionary containing processed features
        """
        pass
    
    @abstractmethod
    async def validate(self, features: Dict[str, Any]) -> bool:
        """Validate processed features.
        
        Args:
            features: Processed features to validate
            
        Returns:
            True if features are valid, False otherwise
        """
        pass
    
    def update_feature_importance(self, importance_scores: Dict[str, float]) -> None:
        """Update feature importance scores.
        
        Args:
            importance_scores: Dictionary mapping feature names to importance scores
        """
        self._feature_importance.update(importance_scores)
        
    def get_feature_stats(self) -> Dict[str, Any]:
        """Get statistics about the feature processor.
        
        Returns:
            Dictionary containing processor statistics
        """
        return {
            "feature_count": len(self._feature_names),
            "last_processed": self.last_processed.isoformat() if self.last_processed else None,
            "importance_scores": self._feature_importance
        } 