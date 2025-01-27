"""Machine learning models and embeddings for the recommendation engine."""

from .embeddings import TextEmbedding, CategoryEmbedding
from .topic_model import TopicModel

__all__ = [
    'TextEmbedding',
    'CategoryEmbedding',
    'TopicModel'
] 