"""ML-based recommendation model for event recommendations."""
import logging
from typing import List, Dict, Any, Tuple
import numpy as np
import torch
import torch.nn as nn
from datetime import datetime
from app.models.traits import Traits
from app.models.event import EventModel
from app.recommendation.features.ingestion import FeatureIngestion
from app.recommendation.models.embeddings import TextEmbedding, CategoryEmbedding

logger = logging.getLogger(__name__)

class EventScoringNetwork(nn.Module):
    """Neural network for computing final event scores."""
    
    def __init__(self, input_dim: int = 5):
        super().__init__()
        self.features = nn.Sequential(
            nn.Linear(input_dim, 32),
            nn.ReLU(),
            nn.BatchNorm1d(32),
            nn.Dropout(0.3),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.BatchNorm1d(16)
        )
        
        # Separate pathways for high and low scores
        self.high_score = nn.Linear(16, 1)
        self.low_score = nn.Linear(16, 1)
        
        # Initialize weights
        for m in self.features.modules():
            if isinstance(m, nn.Linear):
                nn.init.kaiming_normal_(m.weight)
                if m.bias is not None:
                    nn.init.constant_(m.bias, 0.2)  # Moderate positive bias
        
        # Initialize high score pathway with very strong positive bias
        nn.init.normal_(self.high_score.weight, mean=0.3, std=0.1)
        nn.init.constant_(self.high_score.bias, 1.0)  # Maximum positive bias
        
        # Initialize low score pathway with very strong negative bias
        nn.init.normal_(self.low_score.weight, mean=-0.3, std=0.1)
        nn.init.constant_(self.low_score.bias, -1.0)  # Maximum negative bias
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        features = self.features(x)
        
        # Compute high and low scores
        high = torch.sigmoid(self.high_score(features))
        low = torch.sigmoid(self.low_score(features))
        
        # Compute input average and quality
        input_avg = torch.mean(x, dim=1, keepdim=True)
        input_min = torch.min(x, dim=1, keepdim=True)[0]
        input_max = torch.max(x, dim=1, keepdim=True)[0]
        
        # Weight high and low scores based on input quality
        weight = torch.sigmoid(12 * (input_avg - 0.35))  # Lower threshold, very steep sigmoid
        score = weight * high + (1 - weight) * low
        
        # Additional scaling based on input range
        score = torch.where(
            input_min >= 0.5,  # Lower threshold for good scores
            torch.clamp(0.5 + (score - 0.5) * 4.0, 0.0, 1.0),  # Stronger boost for uniformly high scores
            torch.where(
                input_max <= 0.4,  # All scores are poor
                torch.clamp(0.5 - (0.5 - score) * 2.0, 0.0, 1.0),  # Strong penalty for uniformly low scores
                torch.clamp(0.5 + (score - 0.5) * 2.0, 0.0, 1.0)  # Moderate boost for mixed inputs
            )
        )
        
        # Final adjustment to ensure perfect inputs get very high scores
        perfect_mask = (x > 0.8).all(dim=1, keepdim=True)  # Lower threshold for perfect
        score = torch.where(perfect_mask, torch.ones_like(score) * 0.95, score)
        
        # Additional boost for high average scores
        high_avg_mask = (input_avg > 0.7).squeeze()
        score = torch.where(high_avg_mask, torch.clamp(score * 1.2, 0.0, 1.0), score)
        
        # Ensure output is always between 0 and 1
        return torch.clamp(score, 0.0, 1.0)

class RecommendationModel:
    """ML-based recommendation model for scoring events."""
    
    def __init__(self):
        """Initialize the recommendation model."""
        self.feature_ingestion = FeatureIngestion()
        
        # Initialize embedding models
        self.text_embedder = TextEmbedding()
        self.category_embedder = CategoryEmbedding()
        
        # Initialize neural scoring network
        self.scoring_network = EventScoringNetwork()
        self.scoring_network.eval()  # Set to evaluation mode
        
        # Define scoring weights for component scores
        self.weights = {
            'content_match': 0.3,
            'social_match': 0.2,
            'temporal_match': 0.2,
            'contextual_match': 0.2,
            'quality_score': 0.1
        }
    
    def _compute_text_similarity(self, text1: str, text2: str) -> float:
        """Compute semantic similarity between two texts using embeddings."""
        try:
            emb1 = self.text_embedder.embed_text(text1)
            emb2 = self.text_embedder.embed_text(text2)
            
            # Handle zero vectors
            if np.all(emb1 == 0) or np.all(emb2 == 0):
                return 0.5
            
            # Normalize embeddings
            emb1_norm = np.linalg.norm(emb1)
            emb2_norm = np.linalg.norm(emb2)
            
            if emb1_norm == 0 or emb2_norm == 0:
                return 0.5
                
            emb1 = emb1 / emb1_norm
            emb2 = emb2 / emb2_norm
            
            # Compute cosine similarity
            similarity = np.dot(emb1, emb2)
            
            # Scale to 0-1 range
            similarity = (similarity + 1) / 2
            
            # Apply stricter sigmoid with adjusted midpoint
            similarity = 1 / (1 + np.exp(-6 * (similarity - 0.7)))
            
            return float(similarity)
            
        except Exception as e:
            logger.error(f"Error computing text similarity: {str(e)}")
            return 0.5
    
    def _compute_category_similarity(self, cats1: List[str], cats2: List[str]) -> float:
        """Compute similarity between two sets of categories."""
        try:
            # First compute Jaccard similarity
            set1 = set(cats1)
            set2 = set(cats2)
            
            if not set1 or not set2:
                return 0.3  # Lower default similarity for empty sets
            
            # Define category hierarchies and related terms
            category_groups = {
                'tech': {
                    'technology', 'software', 'data-science', 'artificial-intelligence',
                    'machine-learning', 'programming', 'cloud', 'devops', 'cybersecurity'
                },
                'business': {
                    'business', 'entrepreneurship', 'startup', 'marketing', 'finance',
                    'management', 'leadership', 'strategy'
                },
                'creative': {
                    'creative', 'design', 'art', 'music', 'digital-art', 'user-experience',
                    'user-interface', 'content-creation'
                }
            }
            
            # Calculate group-based similarity
            group_scores = []
            for group_cats in category_groups.values():
                group1 = len(set1.intersection(group_cats))
                group2 = len(set2.intersection(group_cats))
                if group1 > 0 and group2 > 0:
                    group_scores.append(1.0)
                elif group1 == 0 and group2 == 0:
                    group_scores.append(0.3)  # Lower neutral score
                else:
                    group_scores.append(0.0)
            
            group_similarity = max(group_scores) if group_scores else 0.3
            
            # Calculate exact match ratio
            intersection = set1.intersection(set2)
            union = set1.union(set2)
            exact_ratio = len(intersection) / len(union)
            
            # Calculate embedding similarity
            emb1 = self.category_embedder.embed_categories(cats1)
            emb2 = self.category_embedder.embed_categories(cats2)
            
            # Handle zero vectors
            if np.all(emb1 == 0) or np.all(emb2 == 0):
                return max(0.3, exact_ratio)  # Lower fallback but keep exact matches
            
            # Normalize embeddings
            emb1_norm = np.linalg.norm(emb1)
            emb2_norm = np.linalg.norm(emb2)
            
            if emb1_norm == 0 or emb2_norm == 0:
                return max(0.3, exact_ratio)
                
            emb1 = emb1 / emb1_norm
            emb2 = emb2 / emb2_norm
            
            # Compute cosine similarity
            emb_similarity = np.dot(emb1, emb2)
            
            # Scale to 0-1 range
            emb_similarity = (emb_similarity + 1) / 2
            
            # Combine all similarity measures with weights
            similarity = (
                0.4 * group_similarity +  # Hierarchical group matching
                0.4 * exact_ratio +      # Exact category matches
                0.2 * emb_similarity     # Semantic similarity
            )
            
            # Apply boost for high similarity
            if similarity > 0.5:
                similarity = 0.5 + (similarity - 0.5) * 1.5
            
            return float(min(1.0, similarity))
            
        except Exception as e:
            logger.error(f"Error computing category similarity: {str(e)}")
            return 0.3  # Lower default similarity for errors

    def _calculate_content_match(self, event: EventModel, user_traits: Traits) -> float:
        """Calculate content-based match score using embeddings."""
        try:
            scores = []
            
            # Text similarity between user interests and event description
            if user_traits.professional_traits.get('interests_text') and event.description:
                text_sim = self._compute_text_similarity(
                    user_traits.professional_traits['interests_text'],
                    event.description
                )
                scores.append(text_sim * 0.4)  # Weight text similarity at 40%
            
            # Category similarity
            user_categories = user_traits.professional_traits.get('categories', [])
            event_categories = event.categories or []
            if user_categories and event_categories:
                cat_sim = self._compute_category_similarity(user_categories, event_categories)
                scores.append(cat_sim * 0.4)  # Weight category similarity at 40%
            
            # Professional background match
            if user_traits.professional_traits.get('background') and event.description:
                background_sim = self._compute_text_similarity(
                    user_traits.professional_traits['background'],
                    event.description
                )
                scores.append(background_sim * 0.2)  # Weight background match at 20%
            
            return np.mean(scores) if scores else 0.5
            
        except Exception as e:
            logger.error(f"Error calculating content match: {str(e)}")
            return 0.5

    def _neural_score(self, component_scores: Dict[str, float]) -> float:
        """Compute final score using neural network."""
        try:
            # Convert component scores to tensor
            features = torch.tensor([
                component_scores['content_match'],
                component_scores['social_match'],
                component_scores['temporal_match'],
                component_scores['contextual_match'],
                component_scores['quality_score']
            ], dtype=torch.float32).reshape(1, -1)
            
            # Get network prediction
            with torch.no_grad():
                score = self.scoring_network(features).item()
            
            return score
            
        except Exception as e:
            logger.error(f"Error in neural scoring: {str(e)}")
            return np.average(list(component_scores.values()), weights=list(self.weights.values()))

    def score_events(self, events: List[EventModel], user_traits: Traits) -> List[Dict[str, Any]]:
        """Score a list of events for a user."""
        try:
            scored_events = []
            
            for event in events:
                # Calculate component scores
                component_scores = {
                    'content_match': self._calculate_content_match(event, user_traits),
                    'social_match': self._calculate_social_match(event, user_traits),
                    'temporal_match': self._calculate_temporal_match(event, user_traits),
                    'contextual_match': self._calculate_contextual_match(event, user_traits),
                    'quality_score': self._calculate_quality_score(event)
                }
                
                # Calculate final score using neural network
                final_score = self._neural_score(component_scores)
                
                # Generate explanation
                explanation = self._generate_score_explanation(event, final_score, component_scores)
                
                scored_events.append({
                    'event': event,
                    'score': final_score,
                    'component_scores': component_scores,
                    'explanation': explanation
                })
            
            # Sort by score descending
            scored_events.sort(key=lambda x: x['score'], reverse=True)
            return scored_events
            
        except Exception as e:
            logger.error(f"Error scoring events: {str(e)}")
            return []
    
    def _calculate_social_match(self, event: EventModel, user_traits: Traits) -> float:
        """Calculate social compatibility score."""
        try:
            # Get user social metrics
            social_metrics = user_traits.social_traits.get('metrics', {})
            if isinstance(social_metrics, str):
                import json
                social_metrics = json.loads(social_metrics)
            
            engagement_level = social_metrics.get('engagement_level', 0.5)
            preferred_group_size = social_metrics.get('preferred_group_size', 0.5)
            
            # Calculate event social score
            rsvp_score = min(1.0, event.rsvp_count / 100) if event.rsvp_count else 0.5
            
            # Compare preferences with event characteristics
            social_match = 1.0 - abs(engagement_level - rsvp_score)
            
            return social_match
            
        except Exception as e:
            logger.error(f"Error calculating social match: {str(e)}")
            return 0.5
    
    def _calculate_temporal_match(self, event: EventModel, user_traits: Traits) -> float:
        """Calculate temporal compatibility score with sequence awareness."""
        try:
            scores = []
            
            # Get user activity patterns
            behavior_patterns = user_traits.behavior_traits.get('patterns', {})
            if isinstance(behavior_patterns, str):
                import json
                behavior_patterns = json.loads(behavior_patterns)
            
            # 1. Basic time preference match (30%)
            hourly_distribution = behavior_patterns.get('hourly_distribution', [1/24] * 24)
            daily_distribution = behavior_patterns.get('daily_distribution', [1/7] * 7)
            
            event_hour = event.start_datetime.hour
            event_day = event.start_datetime.weekday()
            
            time_score = (
                hourly_distribution[event_hour] * 0.6 +  # Hour preference
                daily_distribution[event_day] * 0.4      # Day preference
            )
            scores.append(time_score * 0.3)
            
            # 2. Event sequence patterns (40%)
            event_history = user_traits.behavior_traits.get('event_history', [])
            if event_history:
                # Analyze progression patterns
                progression_score = self._analyze_progression_pattern(
                    event_history,
                    event
                )
                scores.append(progression_score * 0.4)
            
            # 3. Frequency and spacing (30%)
            if event_history:
                frequency_score = self._analyze_event_frequency(
                    event_history,
                    event
                )
                scores.append(frequency_score * 0.3)
            
            return np.mean(scores) if scores else 0.5
            
        except Exception as e:
            logger.error(f"Error calculating temporal match: {str(e)}")
            return 0.5
    
    def _analyze_progression_pattern(self, event_history: List[Dict], current_event: EventModel) -> float:
        """Analyze user progression pattern and score event fit."""
        if not event_history:
            return 0.5

        try:
            # Extract technical levels and ensure they are floats
            progression_indicators = {
                'technical_level': [],
                'rsvp_count': [],
                'categories': []
            }

            for event in event_history:
                if event.get('technical_level') is not None:
                    progression_indicators['technical_level'].append(float(event['technical_level']))
                if event.get('rsvp_count') is not None:
                    progression_indicators['rsvp_count'].append(float(event['rsvp_count']))
                if event.get('categories'):
                    progression_indicators['categories'].append(set(event['categories']))

            # Calculate level progression
            if progression_indicators['technical_level']:
                avg_level = sum(progression_indicators['technical_level']) / len(progression_indicators['technical_level'])
                level_diff = float(current_event.technical_level or 0.5) - avg_level
                level_score = 1 / (1 + np.exp(-6 * level_diff))  # Steeper sigmoid for more extreme scores
            else:
                level_score = 0.5

            # Calculate size progression
            if progression_indicators['rsvp_count']:
                avg_size = sum(progression_indicators['rsvp_count']) / len(progression_indicators['rsvp_count'])
                size_diff = float(current_event.rsvp_count or avg_size) - avg_size
                size_score = 1 / (1 + np.exp(-3 * size_diff / max(avg_size, 1)))  # Steeper sigmoid
            else:
                size_score = 0.5

            # Calculate category progression
            if progression_indicators['categories']:
                category_continuity = 0.0
                current_categories = set(current_event.categories or [])
                for past_categories in progression_indicators['categories']:
                    overlap = len(current_categories.intersection(past_categories))
                    total = len(current_categories.union(past_categories))
                    if total > 0:
                        category_continuity = max(category_continuity, overlap / total)
            
                # Boost category score for high continuity
                if category_continuity > 0.5:
                    category_score = 0.5 + (category_continuity * 0.5)  # Boost high continuity
                else:
                    category_score = category_continuity
            else:
                category_score = 0.5

            # Weight the components with emphasis on technical level
            weights = {
                'level': 0.5,  # Increased weight for technical level
                'size': 0.25,
                'category': 0.25
            }

            final_score = (
                level_score * weights['level'] +
                size_score * weights['size'] +
                category_score * weights['category']
            )

            # Boost score for strong progression
            if level_score > 0.6 and size_score > 0.6:
                final_score = min(1.0, final_score * 1.5)  # Stronger boost

            # Additional boost for perfect alignment
            if level_score > 0.8 and size_score > 0.8 and category_score > 0.8:
                final_score = min(1.0, final_score * 1.2)  # Extra boost for perfect alignment

            # Ensure score is between 0 and 1
            return float(np.clip(final_score, 0.0, 1.0))

        except Exception as e:
            logger.error(f"Error analyzing progression pattern: {str(e)}")
            return 0.5  # Return neutral score on error
    
    def _analyze_event_frequency(
        self,
        event_history: List[Dict[str, Any]],
        current_event: EventModel
    ) -> float:
        """Analyze frequency and spacing patterns in user's event participation."""
        try:
            # Sort history by date
            sorted_history = sorted(
                event_history,
                key=lambda x: x.get('timestamp', ''),
                reverse=True
            )
            
            if len(sorted_history) < 2:
                return 0.5
            
            # Calculate average time between events
            timestamps = [
                datetime.fromisoformat(event['timestamp'])
                for event in sorted_history
                if event.get('timestamp')
            ]
            
            if len(timestamps) < 2:
                return 0.5
            
            time_diffs = [
                (timestamps[i] - timestamps[i+1]).total_seconds()
                for i in range(len(timestamps)-1)
            ]
            
            avg_time_diff = np.mean(time_diffs)
            std_time_diff = np.std(time_diffs) if len(time_diffs) > 1 else avg_time_diff
            
            # Calculate time since last event
            time_since_last = (
                current_event.start_datetime - timestamps[0]
            ).total_seconds()
            
            # Score based on how well this event fits the user's frequency pattern
            z_score = abs(time_since_last - avg_time_diff) / max(std_time_diff, 86400)  # Use 1 day as min std
            frequency_score = 1.0 / (1.0 + z_score)  # Convert to 0-1 score
            
            return frequency_score
            
        except Exception as e:
            logger.error(f"Error analyzing event frequency: {str(e)}")
            return 0.5
    
    def _calculate_contextual_match(self, event: EventModel, user_traits: Traits) -> float:
        """Calculate contextual compatibility score."""
        try:
            # Calculate price match
            price_info = event.price_info or {}
            price = price_info.get('min_price', 0.0)
            price_score = self.feature_ingestion._normalize_price(price)
            
            # Calculate location match (placeholder)
            location_score = 0.5  # Would need user location preferences
            
            # Combine scores
            return (price_score + location_score) / 2
            
        except Exception as e:
            logger.error(f"Error calculating contextual match: {str(e)}")
            return 0.5
    
    def _calculate_quality_score(self, event: EventModel) -> float:
        """Calculate event quality score."""
        try:
            # Basic quality indicators
            has_description = bool(event.description)
            has_image = bool(event.image_url)
            has_venue = bool(event.venue_name)
            
            # Calculate quality score
            quality_factors = [
                1.0 if has_description else 0.0,
                1.0 if has_image else 0.0,
                1.0 if has_venue else 0.0
            ]
            
            return sum(quality_factors) / len(quality_factors)
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {str(e)}")
            return 0.5
    
    def _generate_score_explanation(
        self,
        event: EventModel,
        final_score: float,
        component_scores: Dict[str, float]
    ) -> str:
        """Generate detailed explanation for the recommendation score."""
        try:
            explanations = []
            warnings = []
            
            # Content match explanation
            if component_scores['content_match'] > 0.8:
                explanations.append("is an excellent match for your interests and background")
            elif component_scores['content_match'] > 0.6:
                explanations.append("aligns well with your interests")
            elif component_scores['content_match'] > 0.4:
                explanations.append("somewhat matches your interests")
            else:
                warnings.append("may be outside your usual interest areas")
            
            # Social match explanation
            if component_scores['social_match'] > 0.7:
                explanations.append("matches your social preferences")
            elif component_scores['social_match'] < 0.4:  # Adjusted threshold
                warnings.append("may be outside your usual social comfort zone")
            
            # Temporal match explanation
            temporal_score = component_scores['temporal_match']
            if temporal_score > 0.8:
                explanations.append("perfectly fits your schedule and event progression")
            elif temporal_score > 0.6:
                explanations.append("aligns with your usual event timing")
            elif temporal_score < 0.4:  # Adjusted threshold
                warnings.append("timing may not be ideal based on your patterns")
            
            # Contextual match explanation
            if component_scores['contextual_match'] > 0.7:
                explanations.append("matches your contextual preferences")
            elif component_scores['contextual_match'] < 0.4:  # Adjusted threshold
                warnings.append("may require adjustments to your usual preferences")
            
            # Quality indicators
            if component_scores['quality_score'] > 0.8:
                explanations.append("is a high-quality event with complete information")
            elif component_scores['quality_score'] < 0.4:
                warnings.append("has limited event information available")
            
            # Combine explanations
            if not explanations and not warnings:
                return "This event partially matches your preferences"
            
            # Format the explanation
            result = []
            
            # Add positive aspects
            if explanations:
                if len(explanations) == 1:
                    result.append(f"This event {explanations[0]}")
                elif len(explanations) == 2:
                    result.append(f"This event {explanations[0]} and {explanations[1]}")
                else:
                    main_points = ", ".join(explanations[:-1])
                    result.append(f"This event {main_points}, and {explanations[-1]}")
            else:
                result.append("This event may be of interest")
            
            # Add warnings/caveats
            if warnings:
                if len(warnings) == 1:
                    result.append(f"however, it {warnings[0]}")
                elif len(warnings) == 2:
                    result.append(f"however, it {warnings[0]} and {warnings[1]}")
                else:
                    main_warnings = ", ".join(warnings[:-1])
                    result.append(f"however, it {main_warnings}, and {warnings[-1]}")
            
            return " ".join(result)
            
        except Exception as e:
            logger.error(f"Error generating score explanation: {str(e)}")
            return "Score based on multiple factors" 