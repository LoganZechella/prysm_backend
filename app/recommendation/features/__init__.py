"""Feature engineering and processing for the recommendation engine."""

from .base import BaseFeatureProcessor
from .user_traits import UserTraitProcessor
from .event_traits import EventFeatureProcessor
from .vectorizer import FeatureVectorizer

__all__ = [
    'BaseFeatureProcessor',
    'UserTraitProcessor',
    'EventFeatureProcessor',
    'FeatureVectorizer'
] 