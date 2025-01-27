"""Text and category embedding models."""
from typing import List, Dict, Any, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
import torch
import logging
from pathlib import Path
import json

from app.monitoring.performance import PerformanceMonitor

logger = logging.getLogger(__name__)

class TextEmbedding:
    """Text embedding model using SentenceTransformers."""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        """Initialize the text embedding model.
        
        Args:
            model_name: Name of the sentence-transformers model to use
        """
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)
        self.performance_monitor = PerformanceMonitor()
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        
    def embed_text(self, text: str) -> np.ndarray:
        """Create embedding for a single text.
        
        Args:
            text: Text to embed
            
        Returns:
            Numpy array of embedding
        """
        with self.performance_monitor.monitor_operation('text_embedding'):
            # Normalize and clean text
            text = text.strip().lower()
            if not text:
                return np.zeros(self.embedding_dim)
            
            # Generate embedding
            try:
                embedding = self.model.encode(
                    text,
                    convert_to_numpy=True,
                    normalize_embeddings=True
                )
                return embedding
                
            except Exception as e:
                logger.error(f"Error generating text embedding: {str(e)}")
                return np.zeros(self.embedding_dim)
    
    def embed_batch(self, texts: List[str]) -> np.ndarray:
        """Create embeddings for a batch of texts.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            Numpy array of embeddings
        """
        with self.performance_monitor.monitor_operation('batch_text_embedding'):
            # Clean texts
            texts = [text.strip().lower() for text in texts if text.strip()]
            if not texts:
                return np.zeros((0, self.embedding_dim))
            
            # Generate embeddings
            try:
                embeddings = self.model.encode(
                    texts,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    batch_size=32
                )
                return embeddings
                
            except Exception as e:
                logger.error(f"Error generating batch embeddings: {str(e)}")
                return np.zeros((len(texts), self.embedding_dim))
    
    def embed_with_pooling(
        self,
        texts: List[str],
        weights: Optional[List[float]] = None
    ) -> np.ndarray:
        """Create a single embedding from multiple texts with weighted pooling.
        
        Args:
            texts: List of texts to combine
            weights: Optional weights for each text
            
        Returns:
            Combined embedding vector
        """
        if not texts:
            return np.zeros(self.embedding_dim)
            
        # Generate individual embeddings
        embeddings = self.embed_batch(texts)
        
        # Apply weights and pool
        if weights is None:
            weights = np.ones(len(texts)) / len(texts)
        else:
            weights = np.array(weights) / sum(weights)
        
        # Weighted average
        pooled = np.average(embeddings, axis=0, weights=weights)
        return pooled / np.linalg.norm(pooled)

class CategoryEmbedding:
    """Category embedding using TF-IDF and optional pre-trained embeddings."""
    
    def __init__(
        self,
        vocabulary_path: Optional[str] = None,
        embedding_dim: int = 30
    ):
        """Initialize the category embedding model.
        
        Args:
            vocabulary_path: Optional path to pre-defined category vocabulary
            embedding_dim: Dimension of category embeddings
        """
        self.embedding_dim = embedding_dim
        self.performance_monitor = PerformanceMonitor()
        
        # Initialize vectorizer
        self.vectorizer = TfidfVectorizer(
            lowercase=True,
            max_features=embedding_dim,
            stop_words='english'
        )
        
        # Load pre-defined vocabulary if provided
        if vocabulary_path and Path(vocabulary_path).exists():
            try:
                with open(vocabulary_path, 'r') as f:
                    vocab_data = json.load(f)
                self.vectorizer.vocabulary_ = vocab_data.get('vocabulary', {})
                self.category_embeddings = vocab_data.get('embeddings', {})
            except Exception as e:
                logger.error(f"Error loading category vocabulary: {str(e)}")
                self.category_embeddings = {}
        else:
            self.category_embeddings = {}
    
    def fit(self, categories: List[List[str]]) -> None:
        """Fit the vectorizer on category data.
        
        Args:
            categories: List of category lists to fit on
        """
        with self.performance_monitor.monitor_operation('fit_category_vectorizer'):
            # Flatten and clean categories
            flat_categories = [
                ' '.join(cat.lower() for cat in category_list)
                for category_list in categories
                if category_list
            ]
            
            if not flat_categories:
                return
            
            # Fit vectorizer
            try:
                self.vectorizer.fit(flat_categories)
            except Exception as e:
                logger.error(f"Error fitting category vectorizer: {str(e)}")
    
    def embed_categories(self, categories: List[str]) -> np.ndarray:
        """Create embedding for a list of categories.
        
        Args:
            categories: List of categories to embed
            
        Returns:
            Category embedding vector
        """
        with self.performance_monitor.monitor_operation('embed_categories'):
            if not categories:
                return np.zeros(self.embedding_dim)
            
            # Clean categories
            categories = [cat.lower().strip() for cat in categories]
            
            # Check pre-computed embeddings first
            if all(cat in self.category_embeddings for cat in categories):
                embeddings = [self.category_embeddings[cat] for cat in categories]
                combined = np.mean(embeddings, axis=0)
                return combined / np.linalg.norm(combined)
            
            # Generate new embedding
            try:
                text = ' '.join(categories)
                vector = self.vectorizer.transform([text]).toarray()[0]
                return vector / np.linalg.norm(vector) if np.any(vector) else vector
                
            except Exception as e:
                logger.error(f"Error embedding categories: {str(e)}")
                return np.zeros(self.embedding_dim)
    
    def save_vocabulary(self, path: str) -> None:
        """Save the current vocabulary and embeddings.
        
        Args:
            path: Path to save the vocabulary file
        """
        try:
            data = {
                'vocabulary': self.vectorizer.vocabulary_,
                'embeddings': self.category_embeddings
            }
            with open(path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Error saving category vocabulary: {str(e)}")
    
    def add_category_embedding(
        self,
        category: str,
        embedding: np.ndarray
    ) -> None:
        """Add a pre-computed embedding for a category.
        
        Args:
            category: Category name
            embedding: Pre-computed embedding vector
        """
        if embedding.shape[0] != self.embedding_dim:
            logger.error(f"Invalid embedding dimension: {embedding.shape[0]}")
            return
            
        self.category_embeddings[category.lower().strip()] = embedding.tolist() 