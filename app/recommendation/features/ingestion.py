"""Feature ingestion pipeline for SageMaker Feature Store."""
import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
import pandas as pd
from datetime import datetime
import numpy as np
from typing import Dict, List, Any
import logging
from app.config.aws_config import AWS_CONFIG
import json
from collections import defaultdict
from sagemaker.feature_store.feature_definition import FeatureDefinition, FeatureTypeEnum
import time

logger = logging.getLogger(__name__)

class FeatureIngestion:
    """Handles ingestion of features into SageMaker Feature Store."""
    
    def __init__(self):
        """Initialize feature ingestion with AWS clients."""
        self.region = 'us-east-2'
        self.sagemaker_client = boto3.client('sagemaker', region_name=self.region)
        self.featurestore_runtime = boto3.client('sagemaker-featurestore-runtime', region_name=self.region)
        
        # Initialize SageMaker session
        boto_session = boto3.Session(region_name=self.region)
        self.sagemaker_session = sagemaker.Session(
            boto_session=boto_session,
            sagemaker_client=self.sagemaker_client
        )
        
        # Initialize feature groups
        self._initialize_feature_groups()
    
    def _delete_feature_group(self, feature_group_name: str) -> bool:
        """Delete a feature group if it exists."""
        try:
            # Check if feature group exists
            try:
                response = self.sagemaker_client.describe_feature_group(
                    FeatureGroupName=feature_group_name
                )
                # If group exists and is in failed state, delete it
                if response['FeatureGroupStatus'] in ['CreateFailed', 'DeleteFailed']:
                    logger.info(f"Deleting failed feature group: {feature_group_name}")
                    self.sagemaker_client.delete_feature_group(
                        FeatureGroupName=feature_group_name
                    )
                    # Wait for deletion
                    while True:
                        try:
                            self.sagemaker_client.describe_feature_group(
                                FeatureGroupName=feature_group_name
                            )
                            logger.info(f"Waiting for {feature_group_name} deletion...")
                            time.sleep(10)
                        except self.sagemaker_client.exceptions.ResourceNotFound:
                            logger.info(f"Feature group {feature_group_name} deleted")
                            return True
            except self.sagemaker_client.exceptions.ResourceNotFound:
                return True
            
        except Exception as e:
            logger.error(f"Error deleting feature group {feature_group_name}: {str(e)}")
            return False
        return True
    
    def _wait_for_feature_group_creation(self, feature_group_name: str, timeout: int = 300):
        """Wait for feature group to become active."""
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            try:
                response = self.sagemaker_client.describe_feature_group(
                    FeatureGroupName=feature_group_name
                )
                status = response['FeatureGroupStatus']
                if status == 'Created':
                    logger.info(f"Feature group {feature_group_name} is active")
                    return True
                elif status in ['Creating', 'Updating']:
                    logger.info(f"Feature group {feature_group_name} is {status.lower()}...")
                    time.sleep(10)
                else:
                    logger.error(f"Feature group {feature_group_name} is in unexpected state: {status}")
                    return False
            except Exception as e:
                logger.error(f"Error checking feature group status: {str(e)}")
                return False
        
        logger.error(f"Timeout waiting for feature group {feature_group_name} to become active")
        return False
    
    def _initialize_feature_groups(self):
        """Initialize and create feature groups if they don't exist."""
        try:
            # Define user features schema
            user_features_schema = [
                {'FeatureName': 'user_id', 'FeatureType': 'String'},
                {'FeatureName': 'event_time', 'FeatureType': 'String'},
                {'FeatureName': 'genre_vector', 'FeatureType': 'String'},
                {'FeatureName': 'artist_diversity', 'FeatureType': 'Fractional'},
                {'FeatureName': 'music_recency', 'FeatureType': 'Fractional'},
                {'FeatureName': 'music_consistency', 'FeatureType': 'Fractional'},
                {'FeatureName': 'social_engagement', 'FeatureType': 'Fractional'},
                {'FeatureName': 'collaboration_score', 'FeatureType': 'Fractional'},
                {'FeatureName': 'discovery_score', 'FeatureType': 'Fractional'},
                {'FeatureName': 'professional_interests', 'FeatureType': 'String'},
                {'FeatureName': 'skill_vector', 'FeatureType': 'String'},
                {'FeatureName': 'industry_vector', 'FeatureType': 'String'},
                {'FeatureName': 'activity_times', 'FeatureType': 'String'},
                {'FeatureName': 'engagement_level', 'FeatureType': 'Fractional'},
                {'FeatureName': 'exploration_score', 'FeatureType': 'Fractional'}
            ]
            
            # Define event features schema
            event_features_schema = [
                {'FeatureName': 'event_id', 'FeatureType': 'String'},
                {'FeatureName': 'event_time', 'FeatureType': 'String'},
                {'FeatureName': 'title_embedding', 'FeatureType': 'String'},
                {'FeatureName': 'description_embedding', 'FeatureType': 'String'},
                {'FeatureName': 'category_vector', 'FeatureType': 'String'},
                {'FeatureName': 'topic_vector', 'FeatureType': 'String'},
                {'FeatureName': 'price_normalized', 'FeatureType': 'Fractional'},
                {'FeatureName': 'capacity_normalized', 'FeatureType': 'Fractional'},
                {'FeatureName': 'time_of_day', 'FeatureType': 'Integral'},
                {'FeatureName': 'day_of_week', 'FeatureType': 'Integral'},
                {'FeatureName': 'duration_hours', 'FeatureType': 'Fractional'},
                {'FeatureName': 'attendance_score', 'FeatureType': 'Fractional'},
                {'FeatureName': 'social_score', 'FeatureType': 'Fractional'},
                {'FeatureName': 'organizer_rating', 'FeatureType': 'Fractional'}
            ]
            
            # Check and create user features group
            try:
                response = self.sagemaker_client.describe_feature_group(
                    FeatureGroupName='user-features'
                )
                if response['FeatureGroupStatus'] == 'Created':
                    logger.info("User features group exists and is active")
                else:
                    logger.info(f"User features group exists but status is {response['FeatureGroupStatus']}")
            except self.sagemaker_client.exceptions.ResourceNotFound:
                logger.info("Creating user features group...")
                self.sagemaker_client.create_feature_group(
                    FeatureGroupName='user-features',
                    RecordIdentifierFeatureName='user_id',
                    EventTimeFeatureName='event_time',
                    FeatureDefinitions=user_features_schema,
                    OnlineStoreConfig={'EnableOnlineStore': True},
                    OfflineStoreConfig={
                        'S3StorageConfig': {
                            'S3Uri': f"s3://{AWS_CONFIG['s3_bucket']}/feature-store/user-features"
                        },
                        'DisableGlueTableCreation': True
                    },
                    RoleArn=AWS_CONFIG['feature_store_role_arn'],
                    Description='User features for recommendation system'
                )
                # Wait for user features group to become active
                if not self._wait_for_feature_group_creation('user-features'):
                    raise Exception("Failed to activate user features group")
            
            # Check and create event features group
            try:
                response = self.sagemaker_client.describe_feature_group(
                    FeatureGroupName='event-features'
                )
                if response['FeatureGroupStatus'] == 'Created':
                    logger.info("Event features group exists and is active")
                else:
                    logger.info(f"Event features group exists but status is {response['FeatureGroupStatus']}")
            except self.sagemaker_client.exceptions.ResourceNotFound:
                logger.info("Creating event features group...")
                self.sagemaker_client.create_feature_group(
                    FeatureGroupName='event-features',
                    RecordIdentifierFeatureName='event_id',
                    EventTimeFeatureName='event_time',
                    FeatureDefinitions=event_features_schema,
                    OnlineStoreConfig={'EnableOnlineStore': True},
                    OfflineStoreConfig={
                        'S3StorageConfig': {
                            'S3Uri': f"s3://{AWS_CONFIG['s3_bucket']}/feature-store/event-features"
                        },
                        'DisableGlueTableCreation': True
                    },
                    RoleArn=AWS_CONFIG['feature_store_role_arn'],
                    Description='Event features for recommendation system'
                )
                # Wait for event features group to become active
                if not self._wait_for_feature_group_creation('event-features'):
                    raise Exception("Failed to activate event features group")
            
        except Exception as e:
            logger.error(f"Error initializing feature groups: {str(e)}")
            raise
    
    def process_user_features(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw user data into feature store format."""
        try:
            # Extract and normalize user features
            features = {
                'user_id': str(user_data['id']),
                'event_time': datetime.utcnow().isoformat(),
                
                # Process music preferences
                'genre_vector': json.loads(self._process_genre_preferences(user_data.get('music_preferences', {}))),
                'artist_diversity': self._calculate_artist_diversity(user_data.get('music_history', [])),
                'music_recency': self._calculate_music_recency(user_data.get('music_history', [])),
                'music_consistency': self._calculate_music_consistency(user_data.get('music_history', [])),
                
                # Process social metrics
                'social_engagement': self._calculate_social_engagement(user_data.get('social_data', {})),
                'collaboration_score': self._calculate_collaboration_score(user_data.get('professional_data', {})),
                'discovery_score': self._calculate_discovery_score(user_data),
                
                # Process professional traits
                'professional_interests': json.loads(self._process_professional_interests(user_data.get('professional_data', {}))),
                'skill_vector': json.loads(self._process_skills(user_data.get('professional_data', {}))),
                'industry_vector': json.loads(self._process_industries(user_data.get('professional_data', {}))),
                
                # Process activity patterns
                'activity_times': json.loads(self._process_activity_times(user_data.get('activity_history', []))),
                'engagement_level': self._calculate_engagement_level(user_data),
                'exploration_score': self._calculate_exploration_score(user_data)
            }
            
            return features
            
        except Exception as e:
            logger.error(f"Error processing user features: {str(e)}")
            raise
    
    def process_event_features(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw event data into feature store format."""
        try:
            # Extract and normalize event features
            features = {
                'event_id': str(event_data['id']),
                'event_time': datetime.utcnow().isoformat(),
                
                # Process content embeddings
                'title_embedding': json.loads(self._generate_text_embedding(event_data.get('title', ''))),
                'description_embedding': json.loads(self._generate_text_embedding(event_data.get('description', ''))),
                'category_vector': json.loads(self._process_categories(event_data.get('categories', []))),
                'topic_vector': json.loads(self._extract_topics(event_data.get('description', ''))),
                
                # Process event attributes
                'price_normalized': self._normalize_price(event_data.get('price', 0)),
                'capacity_normalized': self._normalize_capacity(event_data.get('capacity', 0)),
                'time_of_day': self._extract_time_of_day(event_data.get('start_time')),
                'day_of_week': self._extract_day_of_week(event_data.get('start_time')),
                'duration_hours': self._calculate_duration(event_data.get('start_time'), event_data.get('end_time')),
                
                # Process engagement metrics
                'attendance_score': self._calculate_attendance_score(event_data),
                'social_score': self._calculate_social_score(event_data),
                'organizer_rating': self._calculate_organizer_rating(event_data.get('event_history', []))
            }
            
            return features
            
        except Exception as e:
            logger.error(f"Error processing event features: {str(e)}")
            raise
    
    def ingest_user_features(self, user_data: Dict[str, Any]) -> bool:
        """Process and ingest user features into feature store."""
        try:
            # Transform user data into features
            features = self.process_user_features(user_data)
            
            # Convert features to record format expected by Feature Store
            current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            record = {
                'FeatureGroupName': 'user-features',
                'Record': []
            }
            
            # Add each feature with proper formatting
            for key, value in features.items():
                if key == 'event_time':
                    record['Record'].append({
                        'FeatureName': key,
                        'ValueAsString': current_time
                    })
                elif isinstance(value, (int, float)):
                    record['Record'].append({
                        'FeatureName': key,
                        'ValueAsString': str(value)
                    })
                else:
                    record['Record'].append({
                        'FeatureName': key,
                        'ValueAsString': str(value) if not isinstance(value, (dict, list)) else json.dumps(value)
                    })
            
            # Ingest into feature store using the featurestore-runtime client
            self.featurestore_runtime.put_record(**record)
            logger.info(f"Successfully ingested features for user {features['user_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting user features: {str(e)}")
            return False
    
    def ingest_event_features(self, event_data: Dict[str, Any]) -> bool:
        """Process and ingest event features into feature store."""
        try:
            # Transform event data into features
            features = self.process_event_features(event_data)
            
            # Convert features to record format expected by Feature Store
            current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            record = {
                'FeatureGroupName': 'event-features',
                'Record': []
            }
            
            # Add each feature with proper formatting
            for key, value in features.items():
                if key == 'event_time':
                    record['Record'].append({
                        'FeatureName': key,
                        'ValueAsString': current_time
                    })
                elif isinstance(value, (int, float)):
                    record['Record'].append({
                        'FeatureName': key,
                        'ValueAsString': str(value)
                    })
                else:
                    record['Record'].append({
                        'FeatureName': key,
                        'ValueAsString': str(value) if not isinstance(value, (dict, list)) else json.dumps(value)
                    })
            
            # Ingest into feature store using the featurestore-runtime client
            self.featurestore_runtime.put_record(**record)
            logger.info(f"Successfully ingested features for event {features['event_id']}")
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting event features: {str(e)}")
            return False
    
    def batch_ingest_user_features(self, user_data_list: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch process and ingest multiple user features."""
        results = {'success': 0, 'failed': 0}
        
        try:
            # Process all user data into features
            for user_data in user_data_list:
                try:
                    features = self.process_user_features(user_data)
                    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    record = {
                        'FeatureGroupName': 'user-features',
                        'Record': []
                    }
                    
                    # Add each feature with proper formatting
                    for key, value in features.items():
                        if key == 'event_time':
                            record['Record'].append({
                                'FeatureName': key,
                                'ValueAsString': current_time
                            })
                        elif isinstance(value, (int, float)):
                            record['Record'].append({
                                'FeatureName': key,
                                'ValueAsString': str(value)
                            })
                        else:
                            record['Record'].append({
                                'FeatureName': key,
                                'ValueAsString': str(value) if not isinstance(value, (dict, list)) else json.dumps(value)
                            })
                    
                    # Ingest into feature store
                    self.featurestore_runtime.put_record(**record)
                    results['success'] += 1
                    logger.info(f"Successfully ingested features for user {features['user_id']}")
                except Exception as e:
                    logger.error(f"Error ingesting user features: {str(e)}")
                    results['failed'] += 1
            
        except Exception as e:
            logger.error(f"Error in batch ingestion: {str(e)}")
            results['failed'] = len(user_data_list)
        
        return results
    
    def batch_ingest_event_features(self, event_data_list: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch process and ingest multiple event features."""
        results = {'success': 0, 'failed': 0}
        
        try:
            # Process all event data into features
            for event_data in event_data_list:
                try:
                    features = self.process_event_features(event_data)
                    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    record = {
                        'FeatureGroupName': 'event-features',
                        'Record': []
                    }
                    
                    # Add each feature with proper formatting
                    for key, value in features.items():
                        if key == 'event_time':
                            record['Record'].append({
                                'FeatureName': key,
                                'ValueAsString': current_time
                            })
                        elif isinstance(value, (int, float)):
                            record['Record'].append({
                                'FeatureName': key,
                                'ValueAsString': str(value)
                            })
                        else:
                            record['Record'].append({
                                'FeatureName': key,
                                'ValueAsString': str(value) if not isinstance(value, (dict, list)) else json.dumps(value)
                            })
                    
                    # Ingest into feature store
                    self.featurestore_runtime.put_record(**record)
                    results['success'] += 1
                    logger.info(f"Successfully ingested features for event {features['event_id']}")
                except Exception as e:
                    logger.error(f"Error ingesting event features: {str(e)}")
                    results['failed'] += 1
            
        except Exception as e:
            logger.error(f"Error in batch ingestion: {str(e)}")
            results['failed'] = len(event_data_list)
        
        return results
    
    # Helper methods for feature processing
    def _process_genre_preferences(self, preferences: Dict) -> str:
        """Convert genre preferences to a normalized vector string.
        
        Args:
            preferences: Dictionary containing genre preferences with structure:
                {
                    'genres': ['rock', 'jazz', 'electronic', ...],
                    'favorite_artists': ['Artist1', 'Artist2', ...]
                }
                
        Returns:
            JSON string representing normalized genre vector
        """
        try:
            # Get genre list with fallback to empty list
            genres = preferences.get('genres', [])
            
            # Define standard genre categories for normalization
            standard_genres = {
                'rock': ['rock', 'alternative', 'indie', 'metal', 'punk'],
                'electronic': ['electronic', 'dance', 'techno', 'house', 'edm'],
                'jazz': ['jazz', 'blues', 'swing', 'bebop'],
                'classical': ['classical', 'orchestra', 'chamber', 'opera'],
                'pop': ['pop', 'top 40', 'mainstream'],
                'hip_hop': ['hip hop', 'rap', 'r&b', 'trap'],
                'folk': ['folk', 'acoustic', 'singer-songwriter'],
                'world': ['world', 'latin', 'reggae', 'afrobeat'],
                'ambient': ['ambient', 'new age', 'experimental'],
                'country': ['country', 'bluegrass', 'americana']
            }
            
            # Initialize genre vector with zeros
            genre_vector = {genre: 0.0 for genre in standard_genres.keys()}
            
            # Process each user genre
            for user_genre in genres:
                user_genre = user_genre.lower()
                # Find matching standard genre
                for standard_genre, variants in standard_genres.items():
                    if user_genre in variants or user_genre == standard_genre:
                        genre_vector[standard_genre] += 1.0
                        break
            
            # Normalize vector (if any genres are present)
            total = sum(genre_vector.values())
            if total > 0:
                genre_vector = {k: v/total for k, v in genre_vector.items()}
            
            return json.dumps(genre_vector)
            
        except Exception as e:
            logger.error(f"Error processing genre preferences: {str(e)}")
            return "[]"  # Return empty vector on error
    
    def _calculate_artist_diversity(self, history: List) -> float:
        """Calculate artist diversity score based on listening history.
        
        Args:
            history: List of listening history entries with structure:
                [
                    {'artist': 'Artist1', 'track': 'Track1', 'timestamp': '2024-01-27T10:00:00Z'},
                    ...
                ]
                
        Returns:
            Float between 0 and 1 representing artist diversity (higher means more diverse)
        """
        try:
            if not history:
                return 0.0
                
            # Extract all artists from history
            artists = [entry.get('artist', '').strip() for entry in history if entry.get('artist')]
            
            if not artists:
                return 0.0
                
            # Count total plays and unique artists
            total_plays = len(artists)
            unique_artists = len(set(artists))
            
            if total_plays == 0:
                return 0.0
            
            # Calculate diversity metrics
            artist_counts = {}
            for artist in artists:
                artist_counts[artist] = artist_counts.get(artist, 0) + 1
            
            # Calculate normalized Shannon diversity index
            proportions = [count/total_plays for count in artist_counts.values()]
            shannon_diversity = -sum(p * np.log(p) for p in proportions)
            
            # Normalize to 0-1 range (divide by log of number of unique artists)
            max_diversity = np.log(unique_artists) if unique_artists > 1 else 1
            normalized_diversity = min(1.0, shannon_diversity / max_diversity if max_diversity > 0 else 0)
            
            return float(normalized_diversity)
            
        except Exception as e:
            logger.error(f"Error calculating artist diversity: {str(e)}")
            return 0.0
    
    def _calculate_music_recency(self, history: List) -> float:
        """Calculate music recency score based on listening history timestamps.
        
        Args:
            history: List of listening history entries with structure:
                [
                    {'artist': 'Artist1', 'track': 'Track1', 'timestamp': '2024-01-27T10:00:00Z'},
                    ...
                ]
                
        Returns:
            Float between 0 and 1 representing recency (higher means more recent activity)
        """
        try:
            if not history:
                return 0.0
            
            # Extract timestamps
            timestamps = []
            now = datetime.utcnow().replace(tzinfo=None)  # Use naive UTC time
            
            for entry in history:
                try:
                    timestamp = entry.get('timestamp')
                    if timestamp:
                        # Convert to naive UTC time for comparison
                        ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        timestamps.append(ts.replace(tzinfo=None))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid timestamp format: {timestamp}")
                    continue
            
            if not timestamps:
                return 0.0
            
            # Calculate time differences
            time_diffs = [(now - ts).total_seconds() for ts in timestamps]
            
            if not time_diffs:
                return 0.0
            
            # Calculate weighted recency score
            # Weight recent plays more heavily using exponential decay
            # Use 30 days as the half-life
            half_life = 30 * 24 * 3600  # 30 days in seconds
            decay_constant = np.log(2) / half_life
            
            weights = np.exp(-np.array(time_diffs) * decay_constant)
            recency_score = np.mean(weights)
            
            # Normalize to 0-1 range
            return float(recency_score)
            
        except Exception as e:
            logger.error(f"Error calculating music recency: {str(e)}")
            return 0.0
    
    def _calculate_music_consistency(self, history: List) -> float:
        """Calculate music consistency score based on listening patterns.
        
        Args:
            history: List of listening history entries with structure:
                [
                    {'artist': 'Artist1', 'track': 'Track1', 'timestamp': '2024-01-27T10:00:00Z'},
                    ...
                ]
                
        Returns:
            Float between 0 and 1 representing consistency (higher means more consistent patterns)
        """
        try:
            if not history:
                return 0.0
            
            # Extract timestamps and sort chronologically
            timestamps = []
            for entry in history:
                try:
                    timestamp = entry.get('timestamp')
                    if timestamp:
                        timestamps.append(datetime.fromisoformat(timestamp.replace('Z', '+00:00')))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid timestamp format: {timestamp}")
                    continue
            
            if len(timestamps) < 2:  # Need at least 2 points for patterns
                return 0.0
            
            timestamps.sort()
            
            # Calculate time differences between consecutive plays
            time_diffs = np.array([(timestamps[i+1] - timestamps[i]).total_seconds() 
                                 for i in range(len(timestamps)-1)])
            
            # Calculate metrics for consistency
            
            # 1. Regularity of listening intervals
            mean_interval = np.mean(time_diffs)
            std_interval = np.std(time_diffs)
            cv = std_interval / mean_interval if mean_interval > 0 else float('inf')
            
            # Normalize coefficient of variation (lower CV means more consistent)
            # Use a sigmoid function to map CV to 0-1 range
            regularity_score = 1 / (1 + np.exp(cv - 1))
            
            # 2. Daily pattern consistency
            # Convert timestamps to hour of day
            hours = [ts.hour + ts.minute/60 for ts in timestamps]
            
            # Calculate circular variance of hours (0 means perfect consistency, 2 means uniform distribution)
            angles = np.array(hours) * 2 * np.pi / 24
            x = np.mean(np.cos(angles))
            y = np.mean(np.sin(angles))
            r = np.sqrt(x*x + y*y)
            
            # Convert to score (1 - circular variance / 2)
            time_consistency = (1 + r) / 2
            
            # Combine scores (equal weights)
            consistency_score = (regularity_score + time_consistency) / 2
            
            return float(consistency_score)
            
        except Exception as e:
            logger.error(f"Error calculating music consistency: {str(e)}")
            return 0.0
    
    def _calculate_social_engagement(self, social_data: Dict) -> float:
        """Calculate social engagement score based on user's social activity metrics.
        
        Args:
            social_data: Dictionary containing social metrics with structure:
                {
                    'connections': int,  # Number of connections/friends
                    'posts': int,        # Number of posts/shares
                    'comments': int,     # Number of comments
                    'likes': int,        # Number of likes/reactions
                    'groups': int,       # Number of groups/communities
                    'recent_interactions': [  # Last 30 days of interaction data
                        {'type': 'post|comment|like', 'timestamp': '2024-01-27T10:00:00Z'},
                        ...
                    ]
                }
                
        Returns:
            Float between 0 and 1 representing social engagement level
        """
        try:
            if not social_data:
                return 0.0
            
            # Calculate connection score using sigmoid to handle varying network sizes
            # Sigmoid ensures score doesn't grow unbounded for users with many connections
            connections = social_data.get('connections', 0)
            connection_score = 2 / (1 + np.exp(-connections/500)) - 1  # Normalized to 0-1
            
            # Calculate engagement rate from interactions
            total_interactions = (
                social_data.get('posts', 0) + 
                social_data.get('comments', 0) + 
                social_data.get('likes', 0)
            )
            engagement_rate = min(1.0, total_interactions / (connections + 1))  # Normalize by network size
            
            # Calculate activity score from group participation
            groups = social_data.get('groups', 0)
            activity_score = min(1.0, groups / 10)  # Normalize assuming 10+ groups is highly active
            
            # Calculate recency score from recent interactions
            recent_interactions = social_data.get('recent_interactions', [])
            now = datetime.utcnow().replace(tzinfo=None)
            
            recency_scores = []
            for interaction in recent_interactions:
                try:
                    ts = datetime.fromisoformat(interaction['timestamp'].replace('Z', '+00:00'))
                    ts = ts.replace(tzinfo=None)
                    days_ago = (now - ts).days
                    # Weight recent interactions more heavily
                    recency_scores.append(np.exp(-days_ago/30))  # 30-day decay
                except (ValueError, KeyError):
                    continue
            
            recent_interaction_score = np.mean(recency_scores) if recency_scores else 0.0
            
            # Combine scores with weights
            weights = {
                'connection': 0.3,
                'engagement': 0.3,
                'activity': 0.2,
                'recency': 0.2
            }
            
            final_score = (
                weights['connection'] * connection_score +
                weights['engagement'] * engagement_rate +
                weights['activity'] * activity_score +
                weights['recency'] * recent_interaction_score
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating social engagement: {str(e)}")
            return 0.0
    
    def _calculate_collaboration_score(self, professional_data: Dict) -> float:
        """Calculate collaboration tendency score based on professional history.
        
        Args:
            professional_data: Dictionary containing professional metrics with structure:
                {
                    'projects': [
                        {
                            'team_size': int,
                            'role': str,
                            'duration_months': int,
                            'collaborative_tools': List[str]
                        },
                        ...
                    ],
                    'recommendations': [
                        {
                            'skill': str,
                            'timestamp': str,  # ISO format
                            'collaboration_related': bool
                        },
                        ...
                    ],
                    'skills': [
                        {
                            'name': str,
                            'category': str,
                            'endorsements': int
                        },
                        ...
                    ]
                }
                
        Returns:
            Float between 0 and 1 representing collaboration tendency
        """
        try:
            if not professional_data:
                return 0.0
            
            # Calculate project collaboration score
            projects = professional_data.get('projects', [])
            project_scores = []
            
            for project in projects:
                # Base score from team size (sigmoid scaled)
                team_size = project.get('team_size', 1)
                team_score = 2 / (1 + np.exp(-team_size/10)) - 1  # Normalized to 0-1
                
                # Role multiplier (leadership roles weighted more)
                role = project.get('role', '').lower()
                role_multiplier = 1.2 if any(r in role for r in ['lead', 'manager', 'coordinator']) else 1.0
                
                # Duration factor (longer projects indicate sustained collaboration)
                duration = project.get('duration_months', 0)
                duration_factor = min(1.0, duration/24)  # Cap at 2 years
                
                # Tool diversity (more collaborative tools indicate stronger collaboration)
                tools = project.get('collaborative_tools', [])
                tool_factor = min(1.0, len(tools)/5)  # Assume 5+ tools is maximum
                
                project_score = team_score * role_multiplier * (duration_factor + tool_factor)/2
                project_scores.append(project_score)
            
            project_collaboration = np.mean(project_scores) if project_scores else 0.0
            
            # Calculate recommendations score
            recommendations = professional_data.get('recommendations', [])
            collab_recommendations = []
            now = datetime.utcnow().replace(tzinfo=None)
            
            for rec in recommendations:
                if rec.get('collaboration_related', False):
                    try:
                        ts = datetime.fromisoformat(rec['timestamp'].replace('Z', '+00:00'))
                        ts = ts.replace(tzinfo=None)
                        days_ago = (now - ts).days
                        # Weight recent recommendations more heavily
                        collab_recommendations.append(np.exp(-days_ago/365))  # 1-year decay
                    except (ValueError, KeyError):
                        continue
            
            recommendation_score = np.mean(collab_recommendations) if collab_recommendations else 0.0
            
            # Calculate skills diversity score
            skills = professional_data.get('skills', [])
            collaborative_categories = {'leadership', 'communication', 'teamwork', 'project management', 'coordination'}
            
            collaborative_skills = sum(
                1 for skill in skills 
                if skill.get('category', '').lower() in collaborative_categories
            )
            skills_diversity = min(1.0, collaborative_skills / 5)  # Assume 5+ collaborative skills is maximum
            
            # Combine scores with weights
            weights = {
                'project': 0.4,
                'recommendations': 0.3,
                'skills': 0.3
            }
            
            final_score = (
                weights['project'] * project_collaboration +
                weights['recommendations'] * recommendation_score +
                weights['skills'] * skills_diversity
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating collaboration score: {str(e)}")
            return 0.0
    
    def _calculate_discovery_score(self, user_data: Dict) -> float:
        """Calculate discovery tendency score based on user behavior and preferences.
        
        Args:
            user_data: Dictionary containing user activity data with structure:
                {
                    'event_history': [
                        {
                            'category': str,
                            'location': {'lat': float, 'lng': float},
                            'timestamp': str,  # ISO format
                            'is_new_category': bool
                        },
                        ...
                    ],
                    'search_history': [
                        {
                            'query': str,
                            'filters': Dict,
                            'timestamp': str
                        },
                        ...
                    ],
                    'preferences': {
                        'discovery_settings': {
                            'new_categories': bool,
                            'distance_comfort': float,  # in km
                            'novelty_preference': float  # 0-1 score
                        }
                    }
                }
                
        Returns:
            Float between 0 and 1 representing discovery tendency
        """
        try:
            if not user_data:
                return 0.0
            
            # Calculate category exploration score
            event_history = user_data.get('event_history', [])
            total_events = len(event_history)
            if total_events > 0:
                new_category_events = sum(1 for event in event_history if event.get('is_new_category', False))
                category_exploration = new_category_events / total_events
            else:
                category_exploration = 0.0
            
            # Calculate distance exploration score
            if total_events > 1:
                distances = []
                for i in range(1, len(event_history)):
                    try:
                        prev = event_history[i-1]['location']
                        curr = event_history[i]['location']
                        
                        # Calculate distance between consecutive events
                        distance = np.sqrt(
                            (curr['lat'] - prev['lat'])**2 + 
                            (curr['lng'] - prev['lng'])**2
                        ) * 111  # Rough km conversion
                        distances.append(distance)
                    except (KeyError, TypeError):
                        continue
                
                if distances:
                    avg_distance = np.mean(distances)
                    # Normalize distance score (assume 20km is high exploration)
                    distance_exploration = min(1.0, avg_distance / 20)
                else:
                    distance_exploration = 0.0
            else:
                distance_exploration = 0.0
            
            # Calculate temporal diversity score
            if total_events > 0:
                try:
                    timestamps = []
                    for event in event_history:
                        ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
                        hour = ts.hour
                        timestamps.append(hour)
                    
                    # Calculate entropy of event times
                    if timestamps:
                        hour_counts = np.bincount(timestamps, minlength=24)
                        probs = hour_counts / len(timestamps)
                        entropy = -np.sum(p * np.log2(p) if p > 0 else 0 for p in probs)
                        # Normalize entropy (max entropy for 24 hours is ~4.58)
                        temporal_diversity = min(1.0, entropy / 4.58)
                    else:
                        temporal_diversity = 0.0
                except (ValueError, KeyError):
                    temporal_diversity = 0.0
            else:
                temporal_diversity = 0.0
            
            # Calculate search pattern scores
            search_history = user_data.get('search_history', [])
            if search_history:
                # Calculate filter usage score
                total_searches = len(search_history)
                searches_with_filters = sum(1 for search in search_history if search.get('filters'))
                filter_score = 1 - (searches_with_filters / total_searches)  # Less filtering = more discovery
                
                # Calculate query diversity
                queries = [search['query'].lower() for search in search_history if 'query' in search]
                unique_queries = len(set(queries))
                query_diversity = min(1.0, unique_queries / 10)  # Assume 10+ unique queries is high diversity
            else:
                filter_score = 0.5  # Neutral if no search history
                query_diversity = 0.0
            
            # Get explicit discovery preferences
            preferences = user_data.get('preferences', {}).get('discovery_settings', {})
            new_categories_pref = float(preferences.get('new_categories', False))
            distance_comfort = min(1.0, preferences.get('distance_comfort', 5) / 20)  # Normalize to 0-1
            novelty_preference = preferences.get('novelty_preference', 0.5)
            
            # Combine behavior scores
            behavior_score = np.mean([
                category_exploration,
                distance_exploration,
                temporal_diversity,
                filter_score,
                query_diversity
            ])
            
            # Combine preference scores
            preference_score = np.mean([
                new_categories_pref,
                distance_comfort,
                novelty_preference
            ])
            
            # Weight behavior more heavily than stated preferences
            weights = {
                'behavior': 0.7,
                'preferences': 0.3
            }
            
            final_score = (
                weights['behavior'] * behavior_score +
                weights['preferences'] * preference_score
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating discovery score: {str(e)}")
            return 0.0
    
    def _process_professional_interests(self, professional_data: Dict) -> str:
        """Process professional interests into a normalized vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'interests': List[str],
                    'industries': List[str],
                    'roles': List[Dict],  # Current and past roles
                    'certifications': List[Dict],
                    'groups': List[Dict]  # Professional groups/associations
                }
                
        Returns:
            JSON string representing normalized interest vector with metadata
        """
        try:
            if not professional_data:
                return json.dumps({})
            
            # Define standard professional interest categories
            categories = {
                'technology': [
                    'software', 'ai', 'machine learning', 'data science', 'cloud',
                    'cybersecurity', 'blockchain', 'iot', 'devops', 'programming'
                ],
                'business': [
                    'entrepreneurship', 'strategy', 'marketing', 'finance', 'consulting',
                    'management', 'operations', 'sales', 'business development'
                ],
                'creative': [
                    'design', 'art', 'media', 'writing', 'content creation',
                    'ux/ui', 'digital media', 'photography', 'video production'
                ],
                'science': [
                    'research', 'biology', 'physics', 'chemistry', 'mathematics',
                    'data analysis', 'engineering', 'r&d', 'scientific computing'
                ],
                'healthcare': [
                    'medical', 'healthcare', 'biotech', 'pharmaceuticals', 'wellness',
                    'mental health', 'healthcare technology', 'public health'
                ],
                'education': [
                    'teaching', 'training', 'e-learning', 'education technology',
                    'curriculum development', 'academic research', 'mentoring'
                ],
                'social_impact': [
                    'sustainability', 'nonprofit', 'social enterprise', 'environment',
                    'community development', 'social justice', 'philanthropy'
                ],
                'professional_services': [
                    'consulting', 'legal', 'accounting', 'hr', 'recruitment',
                    'project management', 'business analysis', 'risk management'
                ]
            }
            
            # Initialize interest vector
            interest_vector = {category: 0.0 for category in categories.keys()}
            
            # Define weights for different data sources
            weights = {
                'interests': 1.0,      # Explicitly stated interests
                'industries': 0.8,     # Industry experience
                'roles': 0.7,          # Role-based interests
                'certifications': 0.6,  # Professional certifications
                'groups': 0.5          # Group memberships
            }
            
            def match_text_to_categories(text: str) -> List[str]:
                """Helper function to match text against categories."""
                matched = []
                text = text.lower()
                for category, keywords in categories.items():
                    if any(keyword in text for keyword in keywords):
                        matched.append(category)
                return matched
            
            # Process explicit interests
            for interest in professional_data.get('interests', []):
                for category in match_text_to_categories(interest):
                    interest_vector[category] += weights['interests']
            
            # Process industries
            for industry in professional_data.get('industries', []):
                for category in match_text_to_categories(industry):
                    interest_vector[category] += weights['industries']
            
            # Process roles (current and past)
            for role in professional_data.get('roles', []):
                title = role.get('title', '')
                description = role.get('description', '')
                for text in [title, description]:
                    for category in match_text_to_categories(text):
                        interest_vector[category] += weights['roles']
            
            # Process certifications
            for cert in professional_data.get('certifications', []):
                name = cert.get('name', '')
                issuer = cert.get('issuer', '')
                for text in [name, issuer]:
                    for category in match_text_to_categories(text):
                        interest_vector[category] += weights['certifications']
            
            # Process group memberships
            for group in professional_data.get('groups', []):
                name = group.get('name', '')
                description = group.get('description', '')
                for text in [name, description]:
                    for category in match_text_to_categories(text):
                        interest_vector[category] += weights['groups']
            
            # Normalize vector
            total = sum(interest_vector.values())
            if total > 0:
                interest_vector = {k: v/total for k, v in interest_vector.items()}
            
            # Add metadata
            result = {
                'vector': interest_vector,
                'primary_interests': sorted(
                    interest_vector.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:3],
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing professional interests: {str(e)}")
            return json.dumps({})
    
    def _process_skills(self, professional_data: Dict) -> str:
        """Process professional skills into a normalized hierarchical vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'skills': [
                        {
                            'name': str,
                            'category': str,
                            'endorsements': int,
                            'level': str  # beginner, intermediate, expert
                        },
                        ...
                    ],
                    'experience': [
                        {
                            'role': str,
                            'skills_used': List[str],
                            'duration_months': int,
                            'level': str  # junior, mid, senior, lead
                        },
                        ...
                    ]
                }
                
        Returns:
            JSON string representing normalized skill vector with metadata
        """
        try:
            if not professional_data:
                return json.dumps({})
            
            # Define skill categories and their keywords
            skill_categories = {
                'technical': {
                    'programming': [
                        'python', 'java', 'javascript', 'c++', 'ruby', 'go',
                        'rust', 'scala', 'kotlin', 'swift', 'php'
                    ],
                    'web_development': [
                        'html', 'css', 'react', 'angular', 'vue', 'node.js',
                        'django', 'flask', 'spring', 'asp.net'
                    ],
                    'data_science': [
                        'machine learning', 'deep learning', 'nlp', 'computer vision',
                        'statistics', 'data analysis', 'big data', 'tensorflow',
                        'pytorch', 'scikit-learn'
                    ],
                    'devops': [
                        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins',
                        'ci/cd', 'terraform', 'ansible', 'monitoring'
                    ]
                },
                'business': {
                    'management': [
                        'project management', 'team leadership', 'strategy',
                        'operations', 'agile', 'scrum', 'risk management'
                    ],
                    'analysis': [
                        'business analysis', 'requirements gathering', 'data analysis',
                        'market research', 'financial analysis', 'reporting'
                    ],
                    'marketing': [
                        'digital marketing', 'content marketing', 'seo', 'social media',
                        'brand management', 'marketing analytics', 'growth'
                    ]
                },
                'soft_skills': {
                    'communication': [
                        'presentation', 'writing', 'public speaking', 'documentation',
                        'interpersonal', 'client communication'
                    ],
                    'collaboration': [
                        'teamwork', 'mentoring', 'leadership', 'conflict resolution',
                        'cross-functional', 'remote collaboration'
                    ],
                    'problem_solving': [
                        'analytical thinking', 'critical thinking', 'innovation',
                        'creativity', 'decision making', 'troubleshooting'
                    ]
                }
            }
            
            # Initialize hierarchical skill vector
            skill_vector = {
                category: {
                    subcategory: 0.0 
                    for subcategory in subcategories.keys()
                }
                for category, subcategories in skill_categories.items()
            }
            
            def match_skill_to_categories(skill_name: str) -> List[tuple]:
                """Match skill to categories and subcategories."""
                matches = []
                skill_name = skill_name.lower()
                for category, subcategories in skill_categories.items():
                    for subcategory, keywords in subcategories.items():
                        if any(keyword in skill_name for keyword in keywords):
                            matches.append((category, subcategory))
                return matches
            
            # Process explicit skills with endorsements
            skills = professional_data.get('skills', [])
            for skill in skills:
                skill_name = skill.get('name', '')
                endorsements = skill.get('endorsements', 0)
                level_multiplier = {
                    'beginner': 0.5,
                    'intermediate': 1.0,
                    'expert': 1.5
                }.get(skill.get('level', 'intermediate'), 1.0)
                
                # Calculate skill weight based on endorsements and level
                skill_weight = (1 + np.log1p(endorsements)) * level_multiplier
                
                # Update skill vector
                for category, subcategory in match_skill_to_categories(skill_name):
                    skill_vector[category][subcategory] += skill_weight
            
            # Process experience data
            experience = professional_data.get('experience', [])
            for exp in experience:
                duration_months = exp.get('duration_months', 0)
                level_multiplier = {
                    'junior': 0.5,
                    'mid': 1.0,
                    'senior': 1.5,
                    'lead': 2.0
                }.get(exp.get('level', 'mid'), 1.0)
                
                # Calculate experience weight
                exp_weight = (1 + np.log1p(duration_months)) * level_multiplier
                
                # Process skills used in role
                for skill_name in exp.get('skills_used', []):
                    for category, subcategory in match_skill_to_categories(skill_name):
                        skill_vector[category][subcategory] += exp_weight
            
            # Normalize vectors within each category
            for category, subcategories in skill_vector.items():
                total = sum(subcategories.values())
                if total > 0:
                    skill_vector[category] = {
                        k: v/total for k, v in subcategories.items()
                    }
            
            # Calculate category totals for high-level summary
            category_totals = {
                category: sum(subcategories.values())
                for category, subcategories in skill_vector.items()
            }
            total = sum(category_totals.values())
            if total > 0:
                category_totals = {k: v/total for k, v in category_totals.items()}
            
            # Prepare result with detailed vectors and summary
            result = {
                'detailed_vector': skill_vector,
                'category_summary': category_totals,
                'primary_skills': sorted(
                    [
                        (f"{category}.{subcategory}", value)
                        for category, subcategories in skill_vector.items()
                        for subcategory, value in subcategories.items()
                        if value > 0.1  # Only include significant skills
                    ],
                    key=lambda x: x[1],
                    reverse=True
                )[:5],  # Top 5 skills
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing skills: {str(e)}")
            return json.dumps({})
    
    def _process_industries(self, professional_data: Dict) -> str:
        """Process professional industry experience into a normalized vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'industries': [
                        {
                            'name': str,
                            'duration_months': int,
                            'role_level': str  # junior, mid, senior, lead
                        },
                        ...
                    ],
                    'companies': [
                        {
                            'name': str,
                            'industry': str,
                            'size': str,  # startup, mid-size, enterprise
                            'duration_months': int
                        },
                        ...
                    ]
                }
                
        Returns:
            JSON string representing normalized industry vector with metadata
        """
        try:
            if not professional_data:
                return json.dumps({})
            
            # Define standard industry categories and their keywords
            industry_categories = {
                'technology': {
                    'software': [
                        'software', 'saas', 'cloud computing', 'enterprise software',
                        'mobile apps', 'web development', 'cybersecurity'
                    ],
                    'hardware': [
                        'hardware', 'semiconductors', 'electronics', 'robotics',
                        'iot', 'telecommunications', 'networking'
                    ],
                    'internet': [
                        'internet', 'e-commerce', 'digital media', 'social media',
                        'online services', 'streaming', 'digital platforms'
                    ]
                },
                'professional_services': {
                    'consulting': [
                        'consulting', 'professional services', 'business services',
                        'management consulting', 'it consulting', 'advisory'
                    ],
                    'financial': [
                        'financial services', 'banking', 'investment', 'insurance',
                        'fintech', 'wealth management', 'capital markets'
                    ],
                    'legal': [
                        'legal', 'law', 'compliance', 'regulatory', 'intellectual property',
                        'corporate law', 'legal tech'
                    ]
                },
                'healthcare': {
                    'medical': [
                        'healthcare', 'medical', 'hospitals', 'clinics',
                        'healthcare providers', 'medical devices'
                    ],
                    'biotech': [
                        'biotech', 'pharmaceuticals', 'life sciences', 'research',
                        'drug development', 'clinical research'
                    ],
                    'health_tech': [
                        'health tech', 'digital health', 'telemedicine', 'health it',
                        'medical software', 'healthcare analytics'
                    ]
                },
                'media_entertainment': {
                    'media': [
                        'media', 'publishing', 'news', 'broadcasting',
                        'digital media', 'content creation'
                    ],
                    'entertainment': [
                        'entertainment', 'gaming', 'music', 'film', 'television',
                        'streaming services', 'interactive media'
                    ],
                    'advertising': [
                        'advertising', 'marketing', 'pr', 'digital marketing',
                        'brand management', 'creative services'
                    ]
                }
            }
            
            # Initialize industry vector
            industry_vector = {
                category: {
                    subcategory: 0.0
                    for subcategory in subcategories.keys()
                }
                for category, subcategories in industry_categories.items()
            }
            
            def match_industry(industry_name: str) -> List[tuple]:
                """Match industry name to categories and subcategories."""
                matches = []
                industry_name = industry_name.lower()
                for category, subcategories in industry_categories.items():
                    for subcategory, keywords in subcategories.items():
                        if any(keyword in industry_name for keyword in keywords):
                            matches.append((category, subcategory))
                return matches
            
            # Process explicit industry experience
            industries = professional_data.get('industries', [])
            for industry in industries:
                industry_name = industry.get('name', '')
                duration_months = industry.get('duration_months', 0)
                level_multiplier = {
                    'junior': 0.5,
                    'mid': 1.0,
                    'senior': 1.5,
                    'lead': 2.0
                }.get(industry.get('role_level', 'mid'), 1.0)
                
                # Calculate experience weight
                exp_weight = (1 + np.log1p(duration_months)) * level_multiplier
                
                # Update industry vector
                for category, subcategory in match_industry(industry_name):
                    industry_vector[category][subcategory] += exp_weight
            
            # Process company experience
            companies = professional_data.get('companies', [])
            for company in companies:
                industry_name = company.get('industry', '')
                duration_months = company.get('duration_months', 0)
                size_multiplier = {
                    'startup': 1.2,  # More hands-on experience in startups
                    'mid-size': 1.0,
                    'enterprise': 0.8  # More specialized roles in large companies
                }.get(company.get('size', 'mid-size'), 1.0)
                
                # Calculate experience weight
                exp_weight = (1 + np.log1p(duration_months)) * size_multiplier
                
                # Update industry vector
                for category, subcategory in match_industry(industry_name):
                    industry_vector[category][subcategory] += exp_weight
            
            # Normalize vectors within each category
            for category, subcategories in industry_vector.items():
                total = sum(subcategories.values())
                if total > 0:
                    industry_vector[category] = {
                        k: v/total for k, v in subcategories.items()
                    }
            
            # Calculate category totals
            category_totals = {
                category: sum(subcategories.values())
                for category, subcategories in industry_vector.items()
            }
            total = sum(category_totals.values())
            if total > 0:
                category_totals = {k: v/total for k, v in category_totals.items()}
            
            # Prepare result with detailed vectors and summary
            result = {
                'detailed_vector': industry_vector,
                'category_summary': category_totals,
                'primary_industries': sorted(
                    [
                        (f"{category}.{subcategory}", value)
                        for category, subcategories in industry_vector.items()
                        for subcategory, value in subcategories.items()
                        if value > 0.1  # Only include significant experience
                    ],
                    key=lambda x: x[1],
                    reverse=True
                )[:3],  # Top 3 industries
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing industries: {str(e)}")
            return json.dumps({})
    
    def _process_activity_times(self, activity_history: List) -> str:
        """Process activity times into a pattern vector.
        
        Args:
            activity_history: List of activity entries with structure:
                [
                    {
                        'type': str,  # event, search, interaction
                        'timestamp': str,  # ISO format
                        'duration_minutes': int  # optional
                    },
                    ...
                ]
                
        Returns:
            JSON string representing activity patterns with hourly and daily distributions
        """
        try:
            if not activity_history:
                return json.dumps({})
            
            # Initialize time pattern vectors
            hourly_distribution = [0] * 24  # 24 hours
            daily_distribution = [0] * 7   # 7 days of week
            duration_by_time = [0] * 24    # Average duration by hour
            duration_counts = [0] * 24     # Count of activities with duration by hour
            
            # Process each activity
            total_activities = 0
            for activity in activity_history:
                try:
                    # Parse timestamp
                    timestamp = datetime.fromisoformat(
                        activity['timestamp'].replace('Z', '+00:00')
                    )
                    
                    # Update distributions
                    hour = timestamp.hour
                    day = timestamp.weekday()
                    
                    hourly_distribution[hour] += 1
                    daily_distribution[day] += 1
                    total_activities += 1
                    
                    # Process duration if available
                    duration = activity.get('duration_minutes', 0)
                    if duration > 0:
                        duration_by_time[hour] += duration
                        duration_counts[hour] += 1
                    
                except (ValueError, KeyError) as e:
                    logger.warning(f"Invalid timestamp format: {activity.get('timestamp')}")
                    continue
            
            # Normalize distributions
            if total_activities > 0:
                hourly_distribution = [count/total_activities for count in hourly_distribution]
                daily_distribution = [count/total_activities for count in daily_distribution]
            
            # Calculate average duration by hour
            avg_duration_by_time = []
            for hour in range(24):
                if duration_counts[hour] > 0:
                    avg_duration_by_time.append(
                        duration_by_time[hour] / duration_counts[hour]
                    )
                else:
                    avg_duration_by_time.append(0)
            
            # Calculate peak activity times
            peak_hours = sorted(
                range(24),
                key=lambda x: hourly_distribution[x],
                reverse=True
            )[:3]
            
            peak_days = sorted(
                range(7),
                key=lambda x: daily_distribution[x],
                reverse=True
            )[:3]
            
            # Calculate activity clusters
            clusters = []
            current_cluster = []
            threshold = np.mean(hourly_distribution) + np.std(hourly_distribution)
            
            for hour in range(24):
                if hourly_distribution[hour] > threshold:
                    current_cluster.append(hour)
                elif current_cluster:
                    clusters.append(current_cluster)
                    current_cluster = []
            
            if current_cluster:
                clusters.append(current_cluster)
            
            # Prepare result
            result = {
                'hourly_distribution': hourly_distribution,
                'daily_distribution': daily_distribution,
                'avg_duration_by_hour': avg_duration_by_time,
                'peak_hours': peak_hours,
                'peak_days': peak_days,
                'activity_clusters': clusters,
                'metadata': {
                    'total_activities': total_activities,
                    'timestamp': datetime.utcnow().isoformat()
                }
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing activity times: {str(e)}")
            return json.dumps({})
    
    def _calculate_engagement_level(self, user_data: Dict) -> float:
        """Calculate user engagement level based on various activity metrics.
        
        Args:
            user_data: Dictionary containing user activity data with structure:
                {
                    'activity_history': [
                        {
                            'type': str,  # event, search, interaction
                            'timestamp': str,
                            'duration_minutes': int
                        },
                        ...
                    ],
                    'event_interactions': [
                        {
                            'type': str,  # view, save, register, attend
                            'timestamp': str
                        },
                        ...
                    ],
                    'profile_completeness': {
                        'required_fields': int,
                        'optional_fields': int,
                        'filled_required': int,
                        'filled_optional': int
                    },
                    'notification_settings': {
                        'email_enabled': bool,
                        'push_enabled': bool,
                        'categories_selected': List[str]
                    }
                }
                
        Returns:
            Float between 0 and 1 representing overall engagement level
        """
        try:
            if not user_data:
                return 0.0
            
            scores = {}
            
            # 1. Activity Frequency Score
            activity_history = user_data.get('activity_history', [])
            if activity_history:
                # Get timestamps of activities
                timestamps = []
                now = datetime.utcnow().replace(tzinfo=None)
                
                for activity in activity_history:
                    try:
                        ts = datetime.fromisoformat(activity['timestamp'].replace('Z', '+00:00'))
                        ts = ts.replace(tzinfo=None)
                        timestamps.append(ts)
                    except (ValueError, KeyError):
                        continue
                
                if timestamps:
                    # Calculate activity frequency over last 30 days
                    recent_activities = sum(
                        1 for ts in timestamps
                        if (now - ts).days <= 30
                    )
                    # Normalize: assume 20+ activities per month is high engagement
                    scores['frequency'] = min(1.0, recent_activities / 20)
                else:
                    scores['frequency'] = 0.0
            else:
                scores['frequency'] = 0.0
            
            # 2. Event Interaction Score
            event_interactions = user_data.get('event_interactions', [])
            if event_interactions:
                # Weight different interaction types
                interaction_weights = {
                    'view': 0.2,
                    'save': 0.4,
                    'register': 0.7,
                    'attend': 1.0
                }
                
                # Calculate weighted interaction score
                total_score = 0
                total_possible = 0
                
                for interaction in event_interactions:
                    weight = interaction_weights.get(interaction.get('type', 'view'), 0.2)
                    total_score += weight
                    total_possible += 1
                
                if total_possible > 0:
                    # Normalize: assume 10+ interactions is high engagement
                    scores['interaction'] = min(1.0, total_score / (10 * np.mean(list(interaction_weights.values()))))
                else:
                    scores['interaction'] = 0.0
            else:
                scores['interaction'] = 0.0
            
            # 3. Profile Completeness Score
            profile_data = user_data.get('profile_completeness', {})
            if profile_data:
                required_fields = profile_data.get('required_fields', 0)
                optional_fields = profile_data.get('optional_fields', 0)
                filled_required = profile_data.get('filled_required', 0)
                filled_optional = profile_data.get('filled_optional', 0)
                
                if required_fields > 0:
                    required_score = filled_required / required_fields
                else:
                    required_score = 1.0
                
                if optional_fields > 0:
                    optional_score = filled_optional / optional_fields
                else:
                    optional_score = 1.0
                
                # Weight required fields more heavily
                scores['profile'] = (0.7 * required_score) + (0.3 * optional_score)
            else:
                scores['profile'] = 0.0
            
            # 4. Notification Engagement Score
            notification_settings = user_data.get('notification_settings', {})
            if notification_settings:
                # Calculate based on enabled notifications and category selections
                channels_enabled = sum([
                    notification_settings.get('email_enabled', False),
                    notification_settings.get('push_enabled', False)
                ])
                
                categories_selected = len(notification_settings.get('categories_selected', []))
                
                # Normalize: assume 2 channels and 5+ categories is high engagement
                channel_score = channels_enabled / 2
                category_score = min(1.0, categories_selected / 5)
                
                scores['notification'] = (channel_score + category_score) / 2
            else:
                scores['notification'] = 0.0
            
            # Calculate final engagement score with weights
            weights = {
                'frequency': 0.35,    # Activity frequency is most important
                'interaction': 0.30,  # Event interactions are strong indicators
                'profile': 0.20,      # Profile completeness shows investment
                'notification': 0.15  # Notification settings show intent to engage
            }
            
            final_score = sum(
                weights[metric] * score
                for metric, score in scores.items()
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating engagement level: {str(e)}")
            return 0.0
    
    def _calculate_exploration_score(self, user_data: Dict) -> float:
        """Calculate user's exploration tendency based on behavior patterns.
        
        Args:
            user_data: Dictionary containing user behavior data with structure:
                {
                    'event_history': [
                        {
                            'category': str,
                            'location': {'lat': float, 'lng': float},
                            'price': float,
                            'timestamp': str,
                            'is_new_category': bool
                        },
                        ...
                    ],
                    'search_history': [
                        {
                            'query': str,
                            'filters': Dict,
                            'timestamp': str
                        },
                        ...
                    ],
                    'preferences': {
                        'max_distance': float,  # in km
                        'price_range': {'min': float, 'max': float},
                        'preferred_categories': List[str],
                        'excluded_categories': List[str]
                    }
                }
                
        Returns:
            Float between 0 and 1 representing exploration tendency
        """
        try:
            if not user_data:
                return 0.0
            
            scores = {}
            
            # 1. Category Exploration Score
            event_history = user_data.get('event_history', [])
            if event_history:
                # Track unique categories and new category adoption
                categories_seen = set()
                new_category_count = 0
                total_events = len(event_history)
                
                for event in event_history:
                    category = event.get('category')
                    if category:
                        if category not in categories_seen:
                            new_category_count += 1
                            categories_seen.add(category)
                
                if total_events > 0:
                    # Calculate both breadth and willingness to try new categories
                    category_breadth = len(categories_seen) / max(10, total_events)  # Assume 10+ categories is diverse
                    new_category_ratio = new_category_count / total_events
                    scores['category'] = (category_breadth + new_category_ratio) / 2
                else:
                    scores['category'] = 0.0
            else:
                scores['category'] = 0.0
            
            # 2. Location Exploration Score
            if event_history:
                try:
                    # Calculate average distance between consecutive events
                    distances = []
                    for i in range(1, len(event_history)):
                        prev = event_history[i-1].get('location', {})
                        curr = event_history[i].get('location', {})
                        
                        if prev and curr:
                            distance = np.sqrt(
                                (curr['lat'] - prev['lat'])**2 + 
                                (curr['lng'] - prev['lng'])**2
                            ) * 111  # Rough km conversion
                            distances.append(distance)
                    
                    if distances:
                        avg_distance = np.mean(distances)
                        max_distance = user_data.get('preferences', {}).get('max_distance', 20)
                        scores['location'] = min(1.0, avg_distance / max_distance)
                    else:
                        scores['location'] = 0.0
                except (KeyError, TypeError):
                    scores['location'] = 0.0
            else:
                scores['location'] = 0.0
            
            # 3. Price Range Exploration Score
            if event_history:
                try:
                    prices = [event['price'] for event in event_history if 'price' in event]
                    if prices:
                        price_range = user_data.get('preferences', {}).get('price_range', {})
                        min_price = price_range.get('min', min(prices))
                        max_price = price_range.get('max', max(prices))
                        
                        # Calculate how much of the available price range is explored
                        price_coverage = (max(prices) - min(prices)) / (max_price - min_price) if max_price > min_price else 1.0
                        
                        # Calculate price variance normalized by mean
                        mean_price = np.mean(prices)
                        cv = np.std(prices) / mean_price if mean_price > 0 else 0
                        
                        scores['price'] = (price_coverage + min(1.0, cv)) / 2
                    else:
                        scores['price'] = 0.0
                except (KeyError, TypeError):
                    scores['price'] = 0.0
            else:
                scores['price'] = 0.0
            
            # 4. Search Pattern Score
            search_history = user_data.get('search_history', [])
            if search_history:
                # Analyze search patterns
                total_searches = len(search_history)
                unique_queries = len(set(
                    search['query'].lower() 
                    for search in search_history 
                    if 'query' in search
                ))
                
                # Calculate filter flexibility
                filter_counts = []
                for search in search_history:
                    filters = search.get('filters', {})
                    filter_counts.append(len(filters))
                
                if filter_counts:
                    avg_filters = np.mean(filter_counts)
                    # Less filtering indicates more exploration
                    filter_flexibility = 1 - min(1.0, avg_filters / 5)  # Assume 5+ filters is restrictive
                else:
                    filter_flexibility = 1.0
                
                # Combine query diversity and filter flexibility
                query_diversity = unique_queries / total_searches if total_searches > 0 else 0
                scores['search'] = (query_diversity + filter_flexibility) / 2
            else:
                scores['search'] = 0.0
            
            # 5. Category Preference Flexibility
            preferences = user_data.get('preferences', {})
            preferred_categories = set(preferences.get('preferred_categories', []))
            excluded_categories = set(preferences.get('excluded_categories', []))
            
            # Calculate ratio of excluded to total categories
            total_categories = len(preferred_categories | excluded_categories)
            if total_categories > 0:
                flexibility = 1 - (len(excluded_categories) / total_categories)
                scores['flexibility'] = flexibility
            else:
                scores['flexibility'] = 1.0  # No restrictions means maximum flexibility
            
            # Calculate final exploration score with weights
            weights = {
                'category': 0.25,    # Category exploration is key indicator
                'location': 0.20,    # Location variety shows willingness to travel
                'price': 0.15,       # Price range exploration
                'search': 0.25,      # Search behavior is strong indicator
                'flexibility': 0.15  # Preference flexibility
            }
            
            final_score = sum(
                weights[metric] * score
                for metric, score in scores.items()
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating exploration score: {str(e)}")
            return 0.0
    
    def _generate_text_embedding(self, text: str) -> str:
        """Generate text embedding vector using a simple TF-IDF approach.
        
        Args:
            text: Input text to generate embedding for
            
        Returns:
            JSON string representing text embedding vector
        """
        try:
            if not text:
                return json.dumps([])
            
            # Preprocess text
            text = text.lower()
            # Remove special characters and extra whitespace
            text = ' '.join(''.join(c if c.isalnum() or c.isspace() else ' ' for c in text).split())
            
            # Define common words to exclude
            stop_words = {
                'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
                'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
                'to', 'was', 'were', 'will', 'with'
            }
            
            # Split into words and remove stop words
            words = [w for w in text.split() if w not in stop_words]
            
            # Calculate term frequencies
            term_freq = defaultdict(int)
            for word in words:
                term_freq[word] += 1
            
            # Convert to vector (normalized)
            total_terms = sum(term_freq.values())
            if total_terms > 0:
                vector = {word: count/total_terms for word, count in term_freq.items()}
            else:
                vector = {}
            
            return json.dumps(vector)
            
        except Exception as e:
            logger.error(f"Error generating text embedding: {str(e)}")
            return json.dumps([])
    
    def _process_categories(self, categories: List) -> str:
        """Process categories into a normalized vector.
        
        Args:
            categories: List of category strings
            
        Returns:
            JSON string representing category vector
        """
        try:
            if not categories:
                return json.dumps({})
            
            # Define standard category groups
            category_groups = {
                'arts_culture': [
                    'art', 'culture', 'museum', 'gallery', 'exhibition',
                    'theater', 'dance', 'performance', 'film', 'cinema'
                ],
                'business_professional': [
                    'business', 'professional', 'networking', 'career',
                    'entrepreneurship', 'startup', 'workshop', 'seminar'
                ],
                'community_causes': [
                    'community', 'nonprofit', 'charity', 'volunteer',
                    'activism', 'social', 'environment', 'sustainability'
                ],
                'education_learning': [
                    'education', 'learning', 'workshop', 'class', 'training',
                    'lecture', 'conference', 'seminar', 'skills'
                ],
                'entertainment_lifestyle': [
                    'entertainment', 'lifestyle', 'fashion', 'food', 'drink',
                    'music', 'concert', 'festival', 'party', 'nightlife'
                ],
                'health_wellness': [
                    'health', 'wellness', 'fitness', 'yoga', 'meditation',
                    'sports', 'outdoor', 'recreation', 'mental health'
                ],
                'science_tech': [
                    'science', 'technology', 'tech', 'innovation', 'digital',
                    'software', 'data', 'ai', 'robotics', 'engineering'
                ],
                'social_networking': [
                    'social', 'networking', 'meetup', 'gathering', 'club',
                    'group', 'community', 'friends', 'dating'
                ]
            }
            
            # Initialize category vector
            category_vector = {group: 0.0 for group in category_groups.keys()}
            
            # Process each category
            for category in categories:
                category = category.lower()
                # Find matching groups
                for group, keywords in category_groups.items():
                    if any(keyword in category for keyword in keywords):
                        category_vector[group] += 1.0
            
            # Normalize vector
            total = sum(category_vector.values())
            if total > 0:
                category_vector = {k: v/total for k, v in category_vector.items()}
            
            return json.dumps(category_vector)
            
        except Exception as e:
            logger.error(f"Error processing categories: {str(e)}")
            return json.dumps({})
    
    def _extract_topics(self, description: str) -> str:
        """Extract topics from event description using keyword analysis.
        
        Args:
            description: Event description text
            
        Returns:
            JSON string representing topic vector
        """
        try:
            if not description:
                return json.dumps({})
            
            # Define topic keywords and their weights
            topic_keywords = {
                'technology': {
                    'keywords': ['technology', 'tech', 'digital', 'software', 'app',
                               'programming', 'coding', 'developer', 'data', 'ai',
                               'machine learning', 'blockchain', 'cyber', 'cloud'],
                    'weight': 1.0
                },
                'business': {
                    'keywords': ['business', 'entrepreneur', 'startup', 'company',
                               'industry', 'market', 'finance', 'investment',
                               'strategy', 'management', 'leadership'],
                    'weight': 1.0
                },
                'creative': {
                    'keywords': ['art', 'design', 'creative', 'music', 'film',
                               'photography', 'fashion', 'writing', 'performance',
                               'dance', 'theater', 'craft'],
                    'weight': 1.0
                },
                'education': {
                    'keywords': ['education', 'learning', 'training', 'workshop',
                               'course', 'class', 'seminar', 'lecture', 'study',
                               'research', 'academic'],
                    'weight': 1.0
                },
                'social': {
                    'keywords': ['social', 'community', 'networking', 'meetup',
                               'gathering', 'party', 'celebration', 'festival',
                               'entertainment', 'fun'],
                    'weight': 0.8
                },
                'health': {
                    'keywords': ['health', 'wellness', 'fitness', 'exercise',
                               'meditation', 'yoga', 'mindfulness', 'nutrition',
                               'medical', 'healthcare'],
                    'weight': 1.0
                },
                'professional': {
                    'keywords': ['professional', 'career', 'job', 'work',
                               'industry', 'corporate', 'business', 'networking',
                               'skills', 'development'],
                    'weight': 1.0
                },
                'culture': {
                    'keywords': ['culture', 'art', 'history', 'heritage',
                               'tradition', 'language', 'food', 'cuisine',
                               'festival', 'celebration'],
                    'weight': 0.9
                }
            }
            
            # Initialize topic scores
            topic_scores = {topic: 0.0 for topic in topic_keywords.keys()}
            
            # Preprocess description
            description = description.lower()
            
            # Score each topic based on keyword matches
            for topic, data in topic_keywords.items():
                keywords = data['keywords']
                weight = data['weight']
                
                # Count keyword matches
                matches = sum(1 for keyword in keywords if keyword in description)
                # Apply weight and normalize by number of keywords
                topic_scores[topic] = (matches * weight) / len(keywords)
            
            # Normalize scores
            total_score = sum(topic_scores.values())
            if total_score > 0:
                topic_scores = {k: v/total_score for k, v in topic_scores.items()}
            
            # Filter out topics with very low scores
            topic_scores = {k: v for k, v in topic_scores.items() if v > 0.1}
            
            return json.dumps(topic_scores)
            
        except Exception as e:
            logger.error(f"Error extracting topics: {str(e)}")
            return json.dumps({})
    
    def _normalize_price(self, price: float) -> float:
        """Normalize price value to a 0-1 scale.
        
        Args:
            price: Raw price value
            
        Returns:
            Normalized price value between 0 and 1
        """
        try:
            if price < 0:
                return 0.0
            
            # Use log scale for price normalization to handle wide ranges
            # Assume most events are between $0 and $1000
            # Use log(price + 1) to handle free events (price = 0)
            normalized = np.log1p(price) / np.log1p(1000)
            
            # Clip to ensure value is between 0 and 1
            return float(min(1.0, max(0.0, normalized)))
            
        except Exception as e:
            logger.error(f"Error normalizing price: {str(e)}")
            return 0.0
    
    def _normalize_capacity(self, capacity: int) -> float:
        """Normalize capacity value to a 0-1 scale.
        
        Args:
            capacity: Raw capacity value
            
        Returns:
            Normalized capacity value between 0 and 1
        """
        try:
            if capacity < 0:
                return 0.0
            
            # Use log scale for capacity normalization
            # Assume typical events range from 10 to 1000 people
            # Smaller events (< 10) will be normalized to lower values
            # Larger events (> 1000) will be normalized to values close to 1
            normalized = np.log1p(capacity) / np.log1p(1000)
            
            # Clip to ensure value is between 0 and 1
            return float(min(1.0, max(0.0, normalized)))
            
        except Exception as e:
            logger.error(f"Error normalizing capacity: {str(e)}")
            return 0.0
    
    def _extract_time_of_day(self, start_time: str) -> int:
        """Extract hour of day from start time.
        
        Args:
            start_time: ISO format timestamp string
            
        Returns:
            Hour of day (0-23)
        """
        try:
            if not start_time:
                return 0
            
            # Parse ISO format timestamp
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            return dt.hour
            
        except Exception as e:
            logger.error(f"Error extracting time of day: {str(e)}")
            return 0
    
    def _extract_day_of_week(self, start_time: str) -> int:
        """Extract day of week from start time.
        
        Args:
            start_time: ISO format timestamp string
            
        Returns:
            Day of week (0=Monday, 6=Sunday)
        """
        try:
            if not start_time:
                return 0
            
            # Parse ISO format timestamp
            dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            # Convert to 0-6 range where 0 is Monday
            return dt.weekday()
            
        except Exception as e:
            logger.error(f"Error extracting day of week: {str(e)}")
            return 0
    
    def _calculate_duration(self, start_time: str, end_time: str) -> float:
        """Calculate event duration in hours.
        
        Args:
            start_time: ISO format start timestamp
            end_time: ISO format end timestamp
            
        Returns:
            Duration in hours (float)
        """
        try:
            if not start_time or not end_time:
                return 0.0
            
            # Parse ISO format timestamps
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            # Calculate duration in hours
            duration = (end_dt - start_dt).total_seconds() / 3600
            
            # Handle invalid durations (negative or extremely long)
            if duration < 0 or duration > 168:  # Max 1 week
                return 0.0
                
            return float(duration)
            
        except Exception as e:
            logger.error(f"Error calculating duration: {str(e)}")
            return 0.0
    
    def _calculate_attendance_score(self, event_data: Dict) -> float:
        """Calculate attendance score based on event metrics.
        
        Args:
            event_data: Dictionary containing event data with structure:
                {
                    'capacity': int,
                    'registered': int,
                    'waitlist': int,
                    'previous_events': [
                        {
                            'capacity': int,
                            'attendance': int,
                            'satisfaction_score': float
                        },
                        ...
                    ]
                }
                
        Returns:
            Float between 0 and 1 representing attendance score
        """
        try:
            if not event_data:
                return 0.0
            
            scores = {}
            
            # 1. Current Registration Rate
            capacity = event_data.get('capacity', 0)
            registered = event_data.get('registered', 0)
            waitlist = event_data.get('waitlist', 0)
            
            if capacity > 0:
                # Calculate registration rate
                reg_rate = registered / capacity
                # Add bonus for waitlist
                waitlist_bonus = min(0.2, waitlist / capacity)  # Cap bonus at 0.2
                scores['registration'] = min(1.0, reg_rate + waitlist_bonus)
            else:
                scores['registration'] = 0.0
            
            # 2. Historical Attendance Rate
            previous_events = event_data.get('previous_events', [])
            if previous_events:
                attendance_rates = []
                for event in previous_events:
                    event_capacity = event.get('capacity', 0)
                    attendance = event.get('attendance', 0)
                    if event_capacity > 0:
                        attendance_rates.append(attendance / event_capacity)
                
                if attendance_rates:
                    # Use exponential moving average to weight recent events more heavily
                    weights = np.exp(-np.arange(len(attendance_rates)) / 2)  # decay factor of 2
                    weights /= weights.sum()
                    scores['historical'] = float(np.average(attendance_rates, weights=weights))
                else:
                    scores['historical'] = 0.0
            else:
                scores['historical'] = 0.0
            
            # 3. Satisfaction Impact
            if previous_events:
                satisfaction_scores = [
                    event.get('satisfaction_score', 0.0)
                    for event in previous_events
                    if 'satisfaction_score' in event
                ]
                
                if satisfaction_scores:
                    # Use most recent satisfaction scores (last 3)
                    recent_satisfaction = np.mean(satisfaction_scores[-3:])
                    scores['satisfaction'] = float(recent_satisfaction)
                else:
                    scores['satisfaction'] = 0.0
            else:
                scores['satisfaction'] = 0.0
            
            # Calculate final score with weights
            weights = {
                'registration': 0.5,    # Current registration is most important
                'historical': 0.3,      # Historical attendance provides context
                'satisfaction': 0.2     # Satisfaction affects likelihood of attendance
            }
            
            final_score = sum(
                weights[metric] * score
                for metric, score in scores.items()
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating attendance score: {str(e)}")
            return 0.0
    
    def _calculate_social_score(self, event_data: Dict) -> float:
        """Calculate social engagement score for the event.
        
        Args:
            event_data: Dictionary containing event data with structure:
                {
                    'social_metrics': {
                        'shares': int,
                        'likes': int,
                        'comments': int,
                        'rsvps': int
                    },
                    'group_data': {
                        'member_count': int,
                        'active_members': int,
                        'avg_event_engagement': float
                    },
                    'viral_coefficients': {
                        'share_rate': float,
                        'conversion_rate': float
                    }
                }
                
        Returns:
            Float between 0 and 1 representing social engagement score
        """
        try:
            if not event_data:
                return 0.0
            
            scores = {}
            
            # 1. Social Media Engagement
            social_metrics = event_data.get('social_metrics', {})
            if social_metrics:
                # Calculate engagement rate
                total_engagement = (
                    social_metrics.get('shares', 0) * 2.0 +  # Shares weighted more heavily
                    social_metrics.get('likes', 0) * 1.0 +
                    social_metrics.get('comments', 0) * 1.5 +  # Comments show more engagement
                    social_metrics.get('rsvps', 0) * 1.8      # RSVPs show strong intent
                )
                
                # Normalize based on typical engagement rates
                # Assume 100 total weighted engagements is high
                scores['social_engagement'] = min(1.0, total_engagement / 100)
            else:
                scores['social_engagement'] = 0.0
            
            # 2. Group Activity Score
            group_data = event_data.get('group_data', {})
            if group_data:
                member_count = group_data.get('member_count', 0)
                active_members = group_data.get('active_members', 0)
                avg_engagement = group_data.get('avg_event_engagement', 0.0)
                
                if member_count > 0:
                    # Calculate active member ratio
                    activity_ratio = active_members / member_count
                    # Combine with average engagement
                    scores['group_activity'] = (activity_ratio + float(avg_engagement)) / 2
                else:
                    scores['group_activity'] = 0.0
            else:
                scores['group_activity'] = 0.0
            
            # 3. Viral Potential
            viral_data = event_data.get('viral_coefficients', {})
            if viral_data:
                share_rate = viral_data.get('share_rate', 0.0)
                conversion_rate = viral_data.get('conversion_rate', 0.0)
                
                # Calculate viral coefficient (K-factor)
                viral_coefficient = share_rate * conversion_rate
                # Normalize: assume viral coefficient > 1 is very good
                scores['viral'] = min(1.0, viral_coefficient)
            else:
                scores['viral'] = 0.0
            
            # Calculate final score with weights
            weights = {
                'social_engagement': 0.4,  # Direct engagement is most important
                'group_activity': 0.3,     # Group dynamics affect social potential
                'viral': 0.3               # Viral potential indicates broader appeal
            }
            
            final_score = sum(
                weights[metric] * score
                for metric, score in scores.items()
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating social score: {str(e)}")
            return 0.0
    
    def _calculate_organizer_rating(self, event_history: List) -> float:
        """Calculate organizer rating based on event history.
        
        Args:
            event_history: List of historical events with structure:
                [
                    {
                        'attendance_rate': float,
                        'satisfaction_score': float,
                        'review_score': float,
                        'timestamp': str,
                        'cancellation': bool
                    },
                    ...
                ]
                
        Returns:
            Float between 0 and 1 representing organizer rating
        """
        try:
            if not event_history:
                return 0.0
            
            scores = {}
            
            # Sort events by timestamp
            sorted_events = sorted(
                event_history,
                key=lambda x: datetime.fromisoformat(x['timestamp'].replace('Z', '+00:00'))
            )
            
            # 1. Attendance Rate Trend
            attendance_rates = [
                event.get('attendance_rate', 0.0)
                for event in sorted_events
            ]
            
            if attendance_rates:
                # Calculate weighted average with more weight on recent events
                weights = np.exp(np.linspace(-1, 0, len(attendance_rates)))
                weights /= weights.sum()
                scores['attendance'] = float(np.average(attendance_rates, weights=weights))
            else:
                scores['attendance'] = 0.0
            
            # 2. Satisfaction Trend
            satisfaction_scores = [
                event.get('satisfaction_score', 0.0)
                for event in sorted_events
            ]
            
            if satisfaction_scores:
                # Use exponential moving average
                weights = np.exp(np.linspace(-1, 0, len(satisfaction_scores)))
                weights /= weights.sum()
                scores['satisfaction'] = float(np.average(satisfaction_scores, weights=weights))
            else:
                scores['satisfaction'] = 0.0
            
            # 3. Review Score Average
            review_scores = [
                event.get('review_score', 0.0)
                for event in sorted_events
                if 'review_score' in event
            ]
            
            if review_scores:
                # Weight recent reviews more heavily
                weights = np.exp(np.linspace(-1, 0, len(review_scores)))
                weights /= weights.sum()
                scores['reviews'] = float(np.average(review_scores, weights=weights))
            else:
                scores['reviews'] = 0.0
            
            # 4. Reliability Score (based on cancellations)
            total_events = len(sorted_events)
            if total_events > 0:
                cancellations = sum(1 for event in sorted_events if event.get('cancellation', False))
                reliability = 1.0 - (cancellations / total_events)
                scores['reliability'] = float(reliability)
            else:
                scores['reliability'] = 0.0
            
            # Calculate final score with weights
            weights = {
                'attendance': 0.25,     # Consistent attendance shows good organization
                'satisfaction': 0.25,   # Participant satisfaction is key
                'reviews': 0.25,        # Direct feedback from attendees
                'reliability': 0.25     # Reliability in hosting events
            }
            
            final_score = sum(
                weights[metric] * score
                for metric, score in scores.items()
            )
            
            return float(final_score)
            
        except Exception as e:
            logger.error(f"Error calculating organizer rating: {str(e)}")
            return 0.0
    
    def _process_locations(self, professional_data: Dict) -> str:
        """Process location history into a standardized vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'current_location': {
                        'city': str,
                        'state': str,
                        'country': str,
                        'coordinates': {'lat': float, 'lng': float}
                    },
                    'location_history': [
                        {
                            'location': {
                                'city': str,
                                'state': str,
                                'country': str,
                                'coordinates': {'lat': float, 'lng': float}
                            },
                            'duration_months': int,
                            'type': str  # 'work', 'education', 'residence'
                        },
                        ...
                    ],
                    'willing_to_relocate': bool,
                    'preferred_locations': [
                        {
                            'city': str,
                            'state': str,
                            'country': str
                        },
                        ...
                    ]
                }
                
        Returns:
            JSON string representing location preferences and history
        """
        try:
            # Define region mappings
            regions = {
                'north_america': ['us', 'ca', 'mx'],
                'europe': ['gb', 'de', 'fr', 'es', 'it', 'nl', 'se', 'dk', 'no', 'fi'],
                'asia_pacific': ['cn', 'jp', 'kr', 'in', 'au', 'nz', 'sg'],
                'latin_america': ['br', 'ar', 'cl', 'co', 'pe'],
                'middle_east': ['ae', 'sa', 'il', 'tr'],
                'africa': ['za', 'ng', 'ke', 'eg']
            }
            
            # Initialize location data structure
            location_data = {
                'current': {
                    'city': '',
                    'state': '',
                    'country': '',
                    'region': '',
                    'coordinates': {'lat': 0.0, 'lng': 0.0}
                },
                'history': [],
                'mobility': {
                    'willing_to_relocate': False,
                    'international_experience': False,
                    'regions_experienced': set(),
                    'avg_duration_months': 0.0,
                    'moves_frequency': 0.0
                },
                'preferences': {
                    'regions': {},
                    'countries': {},
                    'cities': []
                }
            }
            
            def get_region(country_code: str) -> str:
                """Get region for a country code."""
                country_code = country_code.lower()
                for region, countries in regions.items():
                    if country_code in countries:
                        return region
                return 'other'
            
            # Process current location
            current = professional_data.get('current_location', {})
            if current:
                location_data['current'].update({
                    'city': current.get('city', ''),
                    'state': current.get('state', ''),
                    'country': current.get('country', '').lower(),
                    'region': get_region(current.get('country', '')),
                    'coordinates': current.get('coordinates', {'lat': 0.0, 'lng': 0.0})
                })
            
            # Process location history
            history = professional_data.get('location_history', [])
            total_duration = 0
            moves = 0
            
            for entry in history:
                loc = entry.get('location', {})
                duration = entry.get('duration_months', 0)
                entry_type = entry.get('type', '')
                
                if loc and duration > 0:
                    country = loc.get('country', '').lower()
                    region = get_region(country)
                    
                    location_data['history'].append({
                        'city': loc.get('city', ''),
                        'country': country,
                        'region': region,
                        'duration_months': duration,
                        'type': entry_type
                    })
                    
                    location_data['mobility']['regions_experienced'].add(region)
                    total_duration += duration
                    moves += 1
            
            # Calculate mobility metrics
            if moves > 0:
                location_data['mobility'].update({
                    'avg_duration_months': total_duration / moves,
                    'moves_frequency': moves / (total_duration/12) if total_duration > 0 else 0
                })
            
            location_data['mobility']['international_experience'] = len(location_data['mobility']['regions_experienced']) > 1
            location_data['mobility']['regions_experienced'] = list(location_data['mobility']['regions_experienced'])
            
            # Process relocation preferences
            location_data['mobility']['willing_to_relocate'] = professional_data.get('willing_to_relocate', False)
            
            preferred = professional_data.get('preferred_locations', [])
            region_counts = defaultdict(float)
            country_counts = defaultdict(float)
            
            for loc in preferred:
                country = loc.get('country', '').lower()
                region = get_region(country)
                
                region_counts[region] += 1.0
                country_counts[country] += 1.0
                
                if loc.get('city'):
                    location_data['preferences']['cities'].append({
                        'city': loc['city'],
                        'country': country
                    })
            
            # Normalize preference counts
            if region_counts:
                total = sum(region_counts.values())
                location_data['preferences']['regions'] = {
                    k: v/total for k, v in region_counts.items()
                }
            
            if country_counts:
                total = sum(country_counts.values())
                location_data['preferences']['countries'] = {
                    k: v/total for k, v in country_counts.items()
                }
            
            return json.dumps(location_data)
            
        except Exception as e:
            logger.error(f"Error processing locations: {str(e)}")
            return "[]" 