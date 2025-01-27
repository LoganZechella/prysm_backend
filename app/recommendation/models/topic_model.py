"""Topic modeling for text analysis."""
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from gensim.models import LdaModel
from gensim.corpora import Dictionary
from gensim.utils import simple_preprocess
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
import logging
from pathlib import Path
import json

from app.monitoring.performance import PerformanceMonitor

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

logger = logging.getLogger(__name__)

class TopicModel:
    """Topic modeling using LDA."""
    
    def __init__(
        self,
        num_topics: int = 50,
        model_path: Optional[str] = None
    ):
        """Initialize the topic model.
        
        Args:
            num_topics: Number of topics to extract
            model_path: Optional path to pre-trained model
        """
        self.num_topics = num_topics
        self.performance_monitor = PerformanceMonitor()
        self.stop_words = set(stopwords.words('english'))
        
        # Initialize or load model
        if model_path and Path(model_path).exists():
            try:
                self.dictionary = Dictionary.load(f"{model_path}.dict")
                self.model = LdaModel.load(model_path)
            except Exception as e:
                logger.error(f"Error loading topic model: {str(e)}")
                self._initialize_model()
        else:
            self._initialize_model()
    
    def _initialize_model(self) -> None:
        """Initialize a new LDA model."""
        self.dictionary = Dictionary()
        self.model = LdaModel(
            num_topics=self.num_topics,
            random_state=42,
            update_every=1,
            chunksize=100,
            passes=10,
            alpha='auto',
            per_word_topics=True
        )
    
    def _preprocess_text(self, text: str) -> List[str]:
        """Preprocess text for topic modeling.
        
        Args:
            text: Text to preprocess
            
        Returns:
            List of preprocessed tokens
        """
        # Tokenize and clean
        tokens = word_tokenize(text.lower())
        
        # Remove stopwords and short tokens
        tokens = [
            token for token in tokens
            if token not in self.stop_words and len(token) > 2
        ]
        
        return tokens
    
    def fit(self, texts: List[str]) -> None:
        """Fit the topic model on a corpus of texts.
        
        Args:
            texts: List of texts to fit on
        """
        with self.performance_monitor.monitor_operation('fit_topic_model'):
            if not texts:
                return
            
            try:
                # Preprocess texts
                processed_texts = [
                    self._preprocess_text(text)
                    for text in texts
                    if text.strip()
                ]
                
                # Update dictionary
                self.dictionary.add_documents(processed_texts)
                
                # Convert to bag of words
                corpus = [
                    self.dictionary.doc2bow(text)
                    for text in processed_texts
                ]
                
                # Train model
                self.model.train(
                    corpus,
                    total_examples=len(corpus),
                    epochs=self.model.passes
                )
                
            except Exception as e:
                logger.error(f"Error fitting topic model: {str(e)}")
    
    def extract_topics(
        self,
        text: str,
        threshold: float = 0.1
    ) -> Tuple[np.ndarray, List[Tuple[int, float]]]:
        """Extract topics from text.
        
        Args:
            text: Text to analyze
            threshold: Minimum probability threshold for topics
            
        Returns:
            Tuple of (topic vector, list of (topic_id, probability) pairs)
        """
        with self.performance_monitor.monitor_operation('extract_topics'):
            try:
                # Preprocess text
                tokens = self._preprocess_text(text)
                if not tokens:
                    return np.zeros(self.num_topics), []
                
                # Convert to bag of words
                bow = self.dictionary.doc2bow(tokens)
                
                # Get topic distribution
                topic_dist = self.model.get_document_topics(
                    bow,
                    minimum_probability=threshold
                )
                
                # Create topic vector
                topic_vector = np.zeros(self.num_topics)
                for topic_id, prob in topic_dist:
                    topic_vector[topic_id] = prob
                
                return topic_vector, topic_dist
                
            except Exception as e:
                logger.error(f"Error extracting topics: {str(e)}")
                return np.zeros(self.num_topics), []
    
    def get_topic_terms(
        self,
        topic_id: int,
        num_terms: int = 10
    ) -> List[Tuple[str, float]]:
        """Get the most relevant terms for a topic.
        
        Args:
            topic_id: ID of the topic
            num_terms: Number of terms to return
            
        Returns:
            List of (term, probability) pairs
        """
        try:
            return self.model.show_topic(topic_id, num_terms)
        except Exception as e:
            logger.error(f"Error getting topic terms: {str(e)}")
            return []
    
    def save_model(self, path: str) -> None:
        """Save the current model and dictionary.
        
        Args:
            path: Path to save the model
        """
        try:
            self.model.save(path)
            self.dictionary.save(f"{path}.dict")
        except Exception as e:
            logger.error(f"Error saving topic model: {str(e)}")
    
    def get_topic_coherence(self) -> float:
        """Calculate topic coherence score.
        
        Returns:
            Coherence score between 0 and 1
        """
        try:
            # Get all terms for all topics
            topic_terms = []
            for topic_id in range(self.num_topics):
                terms = [term for term, _ in self.get_topic_terms(topic_id)]
                topic_terms.append(terms)
            
            # Calculate average pairwise similarity
            similarities = []
            for terms in topic_terms:
                if len(terms) < 2:
                    continue
                    
                # Calculate term co-occurrence
                pairs = [(terms[i], terms[j])
                        for i in range(len(terms))
                        for j in range(i+1, len(terms))]
                        
                pair_scores = []
                for term1, term2 in pairs:
                    # Simple co-occurrence score
                    score = self.dictionary.cfs[self.dictionary.token2id[term1]] * \
                           self.dictionary.cfs[self.dictionary.token2id[term2]]
                    pair_scores.append(score)
                
                if pair_scores:
                    similarities.append(np.mean(pair_scores))
            
            return float(np.mean(similarities)) if similarities else 0.0
            
        except Exception as e:
            logger.error(f"Error calculating topic coherence: {str(e)}")
            return 0.0 