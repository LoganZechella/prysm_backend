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
        """Calculate social engagement score based on social activity metrics.
        
        Args:
            social_data: Dictionary containing social metrics with structure:
                {
                    'connections': int,  # Number of connections/friends
                    'engagement_rate': float,  # Average engagement rate (0-1)
                    'activity_score': float,  # Platform-specific activity score (0-1)
                    'recent_interactions': [  # Optional list of recent social interactions
                        {
                            'type': str,  # Type of interaction (comment, like, share, etc.)
                            'timestamp': str,  # ISO format timestamp
                        },
                        ...
                    ]
                }
                
        Returns:
            Float between 0 and 1 representing social engagement level
        """
        try:
            if not social_data:
                return 0.0
            
            scores = []
            
            # 1. Connection score (sigmoid scaled)
            connections = social_data.get('connections', 0)
            connection_score = 2 / (1 + np.exp(-connections/100)) - 1  # Sigmoid centered at 100 connections
            scores.append(connection_score)
            
            # 2. Engagement rate (already 0-1)
            engagement_rate = social_data.get('engagement_rate', 0.0)
            scores.append(float(engagement_rate))
            
            # 3. Activity score (already 0-1)
            activity_score = social_data.get('activity_score', 0.0)
            scores.append(float(activity_score))
            
            # 4. Recent interaction score
            recent_interactions = social_data.get('recent_interactions', [])
            if recent_interactions:
                # Calculate recency-weighted interaction score
                now = datetime.utcnow()
                interaction_scores = []
                
                for interaction in recent_interactions:
                    try:
                        timestamp = datetime.fromisoformat(interaction['timestamp'].replace('Z', '+00:00'))
                        days_ago = (now - timestamp).total_seconds() / (24 * 3600)
                        
                        # Weight interactions by recency (exponential decay with 7-day half-life)
                        weight = np.exp(-np.log(2) * days_ago / 7)
                        interaction_scores.append(weight)
                    except (ValueError, KeyError):
                        continue
                
                if interaction_scores:
                    recent_score = np.mean(interaction_scores)
                    scores.append(recent_score)
            
            # Combine scores with weights
            weights = [0.3, 0.3, 0.2, 0.2]  # Adjust weights based on importance
            if len(scores) < 4:
                # Redistribute weights if we're missing the recent interactions score
                weights = [w/(sum(weights[:len(scores)])) for w in weights[:len(scores)]]
            
            engagement_score = sum(s * w for s, w in zip(scores, weights[:len(scores)]))
            
            return float(min(1.0, max(0.0, engagement_score)))
            
        except Exception as e:
            logger.error(f"Error calculating social engagement: {str(e)}")
            return 0.0
    
    def _calculate_collaboration_score(self, professional_data: Dict) -> float:
        """Calculate collaboration score based on professional activity.
        
        Args:
            professional_data: Dictionary containing professional metrics with structure:
                {
                    'skills': List[str],  # Professional skills
                    'industries': List[str],  # Industries worked in
                    'projects': [  # Optional list of projects
                        {
                            'team_size': int,
                            'role': str,
                            'duration_months': int,
                            'collaboration_type': str  # e.g., 'cross-functional', 'team-lead', 'individual'
                        },
                        ...
                    ],
                    'recommendations': [  # Optional list of professional recommendations
                        {
                            'type': str,  # e.g., 'collaboration', 'leadership', 'technical'
                            'strength': float,  # 0-1 score
                            'timestamp': str
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
            
            scores = []
            
            # 1. Project collaboration score
            projects = professional_data.get('projects', [])
            if projects:
                project_scores = []
                
                for project in projects:
                    # Base score from team size (sigmoid scaled)
                    team_size = project.get('team_size', 1)
                    team_score = 2 / (1 + np.exp(-team_size/5)) - 1  # Sigmoid centered at team size of 5
                    
                    # Role multiplier
                    role_multipliers = {
                        'team-lead': 1.2,
                        'cross-functional': 1.1,
                        'individual': 0.7
                    }
                    role = project.get('role', '').lower()
                    role_multiplier = role_multipliers.get(project.get('collaboration_type', '').lower(), 1.0)
                    
                    # Duration weight (longer projects have more weight)
                    duration_months = project.get('duration_months', 1)
                    duration_weight = min(1.0, duration_months / 12)  # Cap at 1 year
                    
                    project_score = team_score * role_multiplier * duration_weight
                    project_scores.append(project_score)
                
                if project_scores:
                    avg_project_score = np.mean(project_scores)
                    scores.append(min(1.0, avg_project_score))
            
            # 2. Recommendations score
            recommendations = professional_data.get('recommendations', [])
            if recommendations:
                collab_recommendations = [
                    r for r in recommendations 
                    if r.get('type', '').lower() in ['collaboration', 'leadership', 'teamwork']
                ]
                
                if collab_recommendations:
                    # Calculate recency-weighted recommendation score
                    now = datetime.utcnow()
                    weighted_scores = []
                    
                    for rec in collab_recommendations:
                        try:
                            timestamp = datetime.fromisoformat(rec['timestamp'].replace('Z', '+00:00'))
                            days_ago = (now - timestamp).total_seconds() / (24 * 3600)
                            
                            # Weight by recency (1-year half-life) and strength
                            recency_weight = np.exp(-np.log(2) * days_ago / 365)
                            strength = float(rec.get('strength', 0.5))
                            
                            weighted_scores.append(strength * recency_weight)
                        except (ValueError, KeyError):
                            continue
                    
                    if weighted_scores:
                        rec_score = np.mean(weighted_scores)
                        scores.append(rec_score)
            
            # 3. Skills diversity score
            skills = professional_data.get('skills', [])
            if skills:
                # Group skills into categories
                technical_skills = ['programming', 'development', 'engineering', 'technical']
                collaborative_skills = ['communication', 'leadership', 'teamwork', 'management']
                
                skill_counts = {
                    'technical': sum(1 for s in skills if any(t in s.lower() for t in technical_skills)),
                    'collaborative': sum(1 for s in skills if any(c in s.lower() for c in collaborative_skills))
                }
                
                total_skills = sum(skill_counts.values())
                if total_skills > 0:
                    # Calculate ratio of collaborative to total skills
                    skill_ratio = skill_counts['collaborative'] / total_skills
                    scores.append(skill_ratio)
            
            # Combine scores with weights
            weights = [0.5, 0.3, 0.2]  # Projects, recommendations, skills
            if len(scores) < 3:
                # Redistribute weights if we're missing some scores
                weights = [w/(sum(weights[:len(scores)])) for w in weights[:len(scores)]]
            
            collaboration_score = sum(s * w for s, w in zip(scores, weights[:len(scores)]))
            
            return float(min(1.0, max(0.0, collaboration_score)))
            
        except Exception as e:
            logger.error(f"Error calculating collaboration score: {str(e)}")
            return 0.0
    
    def _calculate_discovery_score(self, user_data: Dict) -> float:
        """Calculate discovery score based on user's exploration patterns.
        
        Args:
            user_data: Dictionary containing user activity data with structure:
                {
                    'event_history': [  # Past event attendance
                        {
                            'event_id': str,
                            'category': str,
                            'timestamp': str,
                            'is_new_category': bool,  # Whether this was first time in category
                            'distance_km': float,  # Distance from usual location
                        },
                        ...
                    ],
                    'search_history': [  # Optional search patterns
                        {
                            'query': str,
                            'timestamp': str,
                            'filters_used': List[str]
                        },
                        ...
                    ],
                    'preferences': {  # Optional stated preferences
                        'discovery_settings': {
                            'distance_willing': float,  # km
                            'category_diversity': float,  # 0-1
                            'novelty_preference': float  # 0-1
                        }
                    }
                }
                
        Returns:
            Float between 0 and 1 representing discovery tendency
        """
        try:
            if not user_data:
                return 0.0
            
            scores = []
            
            # 1. Event history analysis
            event_history = user_data.get('event_history', [])
            if event_history:
                # Calculate metrics from event history
                
                # a) Category exploration score
                new_category_ratio = sum(1 for e in event_history if e.get('is_new_category', False)) / len(event_history)
                scores.append(new_category_ratio)
                
                # b) Distance exploration score
                distances = [e.get('distance_km', 0) for e in event_history]
                if distances:
                    # Normalize distances using sigmoid
                    avg_distance = np.mean(distances)
                    distance_score = 2 / (1 + np.exp(-avg_distance/10)) - 1  # Sigmoid centered at 10km
                    scores.append(distance_score)
                
                # c) Temporal diversity
                try:
                    timestamps = [
                        datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00'))
                        for e in event_history
                        if e.get('timestamp')
                    ]
                    if len(timestamps) >= 2:
                        # Calculate variance in time between events
                        time_diffs = np.array([(timestamps[i+1] - timestamps[i]).total_seconds() 
                                             for i in range(len(timestamps)-1)])
                        cv = np.std(time_diffs) / np.mean(time_diffs) if np.mean(time_diffs) > 0 else 0
                        temporal_score = 1 / (1 + cv)  # Higher variance = higher discovery score
                        scores.append(temporal_score)
                except (ValueError, KeyError):
                    pass
            
            # 2. Search pattern analysis
            search_history = user_data.get('search_history', [])
            if search_history:
                # Analyze search diversity
                
                # a) Filter usage diversity
                all_filters = set()
                for search in search_history:
                    all_filters.update(search.get('filters_used', []))
                
                filter_score = min(1.0, len(all_filters) / 5)  # Normalize to max of 5 different filters
                scores.append(filter_score)
                
                # b) Query diversity
                queries = [s.get('query', '').lower() for s in search_history]
                unique_queries = len(set(queries))
                query_diversity = min(1.0, unique_queries / len(queries)) if queries else 0
                scores.append(query_diversity)
            
            # 3. Stated preferences
            preferences = user_data.get('preferences', {}).get('discovery_settings', {})
            if preferences:
                # Combine explicit preference indicators
                pref_scores = []
                
                # Distance willingness (normalize to 50km max)
                distance_willing = preferences.get('distance_willing', 0)
                distance_pref = min(1.0, distance_willing / 50)
                pref_scores.append(distance_pref)
                
                # Category diversity preference (already 0-1)
                category_pref = preferences.get('category_diversity', 0.5)
                pref_scores.append(float(category_pref))
                
                # Novelty preference (already 0-1)
                novelty_pref = preferences.get('novelty_preference', 0.5)
                pref_scores.append(float(novelty_pref))
                
                if pref_scores:
                    avg_pref_score = np.mean(pref_scores)
                    scores.append(avg_pref_score)
            
            # Combine all scores with weights
            # Prioritize actual behavior over stated preferences
            weights = {
                'event_history': 0.4,
                'search_patterns': 0.4,
                'stated_preferences': 0.2
            }
            
            # Group scores by category
            grouped_scores = {
                'event_history': scores[:3] if len(scores) >= 3 else [],
                'search_patterns': scores[3:5] if len(scores) >= 5 else [],
                'stated_preferences': scores[5:] if len(scores) >= 6 else []
            }
            
            # Calculate weighted average for each category
            category_scores = {}
            for category, category_scores_list in grouped_scores.items():
                if category_scores_list:
                    category_scores[category] = np.mean(category_scores_list)
            
            # Calculate final weighted score
            if category_scores:
                total_weight = sum(weights[cat] for cat in category_scores.keys())
                discovery_score = sum(
                    score * (weights[cat]/total_weight)
                    for cat, score in category_scores.items()
                )
            else:
                discovery_score = 0.0
            
            return float(min(1.0, max(0.0, discovery_score)))
            
        except Exception as e:
            logger.error(f"Error calculating discovery score: {str(e)}")
            return 0.0
    
    def _process_professional_interests(self, professional_data: Dict) -> str:
        """Process professional interests into a standardized vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'interests': List[str],  # Professional interests
                    'industries': List[str],  # Industries worked in
                    'roles': List[str],  # Past/current roles
                    'certifications': List[str],  # Optional professional certifications
                    'groups': List[str]  # Optional professional group memberships
                }
                
        Returns:
            JSON string representing normalized interest vector
        """
        try:
            # Define standard professional interest categories
            interest_categories = {
                'technology': [
                    'software', 'programming', 'ai', 'machine learning', 'data science',
                    'cloud', 'cybersecurity', 'blockchain', 'iot', 'devops'
                ],
                'business': [
                    'entrepreneurship', 'management', 'strategy', 'consulting',
                    'marketing', 'sales', 'finance', 'operations', 'leadership'
                ],
                'creative': [
                    'design', 'ux', 'ui', 'content', 'writing', 'media',
                    'art', 'photography', 'video', 'animation'
                ],
                'science': [
                    'research', 'analytics', 'mathematics', 'physics', 'biology',
                    'chemistry', 'engineering', 'robotics', 'space'
                ],
                'healthcare': [
                    'medical', 'health', 'biotech', 'pharma', 'wellness',
                    'mental health', 'healthcare', 'nutrition'
                ],
                'education': [
                    'teaching', 'training', 'coaching', 'mentoring', 'e-learning',
                    'education', 'academic', 'learning'
                ],
                'social_impact': [
                    'nonprofit', 'sustainability', 'social enterprise', 'environment',
                    'community', 'advocacy', 'policy', 'social justice'
                ],
                'professional_services': [
                    'legal', 'accounting', 'hr', 'recruiting', 'consulting',
                    'project management', 'business analysis'
                ]
            }
            
            # Initialize interest vector
            interest_vector = {category: 0.0 for category in interest_categories.keys()}
            
            # Process each data source with different weights
            weights = {
                'interests': 1.0,
                'industries': 0.8,
                'roles': 0.6,
                'certifications': 0.4,
                'groups': 0.3
            }
            
            # Helper function to match text against categories
            def match_categories(text: str) -> List[str]:
                text = text.lower()
                matches = []
                for category, keywords in interest_categories.items():
                    if any(keyword in text for keyword in keywords):
                        matches.append(category)
                return matches
            
            # Process each data source
            for source, weight in weights.items():
                items = professional_data.get(source, [])
                for item in items:
                    matches = match_categories(item)
                    for category in matches:
                        interest_vector[category] += weight
            
            # Normalize vector
            total = sum(interest_vector.values())
            if total > 0:
                interest_vector = {k: v/total for k, v in interest_vector.items()}
            
            # Add metadata
            result = {
                'vector': interest_vector,
                'primary_interests': [
                    k for k, v in sorted(interest_vector.items(), key=lambda x: x[1], reverse=True)
                    if v > 0.1  # Only include significant interests
                ][:3]  # Top 3 interests
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing professional interests: {str(e)}")
            return "[]"
    
    def _process_skills(self, professional_data: Dict) -> str:
        """Process professional skills into a standardized vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'skills': List[str],  # Professional skills
                    'endorsements': Dict[str, int],  # Optional skill endorsements
                    'experience': [  # Optional detailed experience
                        {
                            'skills_used': List[str],
                            'duration_months': int,
                            'level': str  # e.g., 'beginner', 'intermediate', 'expert'
                        },
                        ...
                    ]
                }
                
        Returns:
            JSON string representing normalized skill vector
        """
        try:
            # Define standard skill categories and their keywords
            skill_categories = {
                'technical': {
                    'programming': [
                        'python', 'java', 'javascript', 'c++', 'ruby', 'php',
                        'golang', 'rust', 'scala', 'swift', 'kotlin'
                    ],
                    'web_development': [
                        'html', 'css', 'react', 'angular', 'vue', 'node.js',
                        'django', 'flask', 'spring', 'asp.net'
                    ],
                    'data': [
                        'sql', 'mongodb', 'postgresql', 'mysql', 'elasticsearch',
                        'hadoop', 'spark', 'tableau', 'power bi'
                    ],
                    'cloud': [
                        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform',
                        'jenkins', 'ci/cd', 'devops'
                    ],
                    'ai_ml': [
                        'machine learning', 'deep learning', 'tensorflow', 'pytorch',
                        'nlp', 'computer vision', 'data science'
                    ]
                },
                'business': {
                    'management': [
                        'project management', 'team leadership', 'agile', 'scrum',
                        'strategic planning', 'risk management'
                    ],
                    'analysis': [
                        'business analysis', 'requirements gathering', 'process improvement',
                        'data analysis', 'market research'
                    ],
                    'communication': [
                        'presentation', 'public speaking', 'technical writing',
                        'client relations', 'stakeholder management'
                    ]
                },
                'soft_skills': {
                    'collaboration': [
                        'teamwork', 'cross-functional', 'mentoring', 'leadership',
                        'conflict resolution'
                    ],
                    'problem_solving': [
                        'analytical thinking', 'critical thinking', 'innovation',
                        'creativity', 'decision making'
                    ],
                    'work_management': [
                        'time management', 'organization', 'multitasking',
                        'attention to detail', 'adaptability'
                    ]
                }
            }
            
            # Initialize skill vector with hierarchical structure
            skill_vector = {
                category: {
                    subcategory: 0.0 
                    for subcategory in subcategories.keys()
                }
                for category, subcategories in skill_categories.items()
            }
            
            def match_skill(skill: str) -> List[tuple]:
                """Match a skill to categories and subcategories."""
                skill = skill.lower()
                matches = []
                for category, subcategories in skill_categories.items():
                    for subcategory, keywords in subcategories.items():
                        if any(keyword in skill for keyword in keywords):
                            matches.append((category, subcategory))
                return matches
            
            # Process skills with endorsements
            skills = professional_data.get('skills', [])
            endorsements = professional_data.get('endorsements', {})
            
            for skill in skills:
                # Get endorsement count (default to 1 if no endorsements)
                endorsement_weight = min(1.0 + np.log1p(endorsements.get(skill, 0)), 5.0)
                
                # Match skill to categories
                matches = match_skill(skill)
                for category, subcategory in matches:
                    skill_vector[category][subcategory] += endorsement_weight
            
            # Process experience
            experience = professional_data.get('experience', [])
            for exp in experience:
                # Calculate experience weight based on duration and level
                duration_months = exp.get('duration_months', 0)
                level_multipliers = {
                    'beginner': 0.5,
                    'intermediate': 1.0,
                    'expert': 1.5
                }
                level = exp.get('level', 'intermediate').lower()
                exp_weight = min(1.0 + np.log1p(duration_months/12), 3.0) * level_multipliers.get(level, 1.0)
                
                # Process skills used in this experience
                for skill in exp.get('skills_used', []):
                    matches = match_skill(skill)
                    for category, subcategory in matches:
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
            
            # Prepare final result with metadata
            result = {
                'detailed_vector': skill_vector,
                'category_summary': category_totals,
                'primary_skills': [
                    {
                        'category': category,
                        'subcategories': [
                            k for k, v in sorted(subcategories.items(), key=lambda x: x[1], reverse=True)
                            if v > 0.1
                        ][:2]  # Top 2 subcategories per category
                    }
                    for category, subcategories in skill_vector.items()
                    if sum(subcategories.values()) > 0
                ]
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing skills: {str(e)}")
            return "[]"
    
    def _process_industries(self, professional_data: Dict) -> str:
        """Process industries into a standardized vector.
        
        Args:
            professional_data: Dictionary containing professional data with structure:
                {
                    'industries': List[str],  # Industries worked in
                    'experience': [  # Optional detailed experience
                        {
                            'industry': str,
                            'duration_months': int,
                            'role_level': str  # e.g., 'entry', 'mid', 'senior', 'executive'
                        },
                        ...
                    ],
                    'interests': List[str]  # Optional industry interests
                }
                
        Returns:
            JSON string representing normalized industry vector
        """
        try:
            # Define standard industry categories and their keywords
            industry_categories = {
                'technology': {
                    'software': [
                        'software', 'saas', 'enterprise software', 'mobile apps',
                        'web development', 'cloud computing'
                    ],
                    'hardware': [
                        'hardware', 'electronics', 'semiconductors', 'iot',
                        'robotics', 'telecommunications'
                    ],
                    'internet': [
                        'internet', 'e-commerce', 'digital media', 'social media',
                        'online services', 'digital platforms'
                    ]
                },
                'business_services': {
                    'consulting': [
                        'consulting', 'professional services', 'business services',
                        'management consulting', 'strategy consulting'
                    ],
                    'financial': [
                        'banking', 'finance', 'investment', 'insurance',
                        'fintech', 'wealth management'
                    ],
                    'marketing': [
                        'marketing', 'advertising', 'pr', 'market research',
                        'digital marketing', 'branding'
                    ]
                },
                'healthcare': {
                    'medical': [
                        'healthcare', 'hospitals', 'medical devices', 'pharmaceuticals',
                        'biotechnology', 'health services'
                    ],
                    'wellness': [
                        'wellness', 'fitness', 'mental health', 'nutrition',
                        'healthcare technology', 'telehealth'
                    ]
                },
                'education': {
                    'traditional': [
                        'education', 'higher education', 'k-12', 'academic',
                        'schools', 'universities'
                    ],
                    'edtech': [
                        'edtech', 'e-learning', 'online education', 'training',
                        'professional development', 'educational services'
                    ]
                },
                'manufacturing': {
                    'traditional': [
                        'manufacturing', 'industrial', 'automotive', 'aerospace',
                        'machinery', 'construction'
                    ],
                    'advanced': [
                        'advanced manufacturing', '3d printing', 'smart manufacturing',
                        'industrial automation', 'industry 4.0'
                    ]
                }
            }
            
            # Initialize industry vector with hierarchical structure
            industry_vector = {
                category: {
                    subcategory: 0.0 
                    for subcategory in subcategories.keys()
                }
                for category, subcategories in industry_categories.items()
            }
            
            def match_industry(industry: str) -> List[tuple]:
                """Match an industry to categories and subcategories."""
                industry = industry.lower()
                matches = []
                for category, subcategories in industry_categories.items():
                    for subcategory, keywords in subcategories.items():
                        if any(keyword in industry for keyword in keywords):
                            matches.append((category, subcategory))
                return matches
            
            # Process listed industries
            industries = professional_data.get('industries', [])
            for industry in industries:
                matches = match_industry(industry)
                for category, subcategory in matches:
                    industry_vector[category][subcategory] += 1.0
            
            # Process experience with weights
            experience = professional_data.get('experience', [])
            for exp in experience:
                # Calculate experience weight based on duration and role level
                duration_months = exp.get('duration_months', 0)
                level_multipliers = {
                    'entry': 0.5,
                    'mid': 1.0,
                    'senior': 1.5,
                    'executive': 2.0
                }
                level = exp.get('role_level', 'mid').lower()
                exp_weight = min(1.0 + np.log1p(duration_months/12), 3.0) * level_multipliers.get(level, 1.0)
                
                # Process industry experience
                industry = exp.get('industry', '')
                if industry:
                    matches = match_industry(industry)
                    for category, subcategory in matches:
                        industry_vector[category][subcategory] += exp_weight
            
            # Process interests with lower weight
            interests = professional_data.get('interests', [])
            for interest in interests:
                matches = match_industry(interest)
                for category, subcategory in matches:
                    industry_vector[category][subcategory] += 0.5
            
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
            
            # Prepare final result with metadata
            result = {
                'detailed_vector': industry_vector,
                'category_summary': category_totals,
                'primary_industries': [
                    {
                        'category': category,
                        'subcategories': [
                            k for k, v in sorted(subcategories.items(), key=lambda x: x[1], reverse=True)
                            if v > 0.1
                        ][:2]  # Top 2 subcategories per category
                    }
                    for category, subcategories in industry_vector.items()
                    if sum(subcategories.values()) > 0
                ]
            }
            
            return json.dumps(result)
            
        except Exception as e:
            logger.error(f"Error processing industries: {str(e)}")
            return "[]"
    
    def _process_activity_times(self, activity_history: List) -> str:
        """Process activity times into a pattern vector."""
        # TODO: Implement activity times processing
        return "[]"  # Placeholder
    
    def _calculate_engagement_level(self, user_data: Dict) -> float:
        """Calculate user engagement level."""
        # TODO: Implement engagement level calculation
        return 0.0  # Placeholder
    
    def _calculate_exploration_score(self, user_data: Dict) -> float:
        """Calculate exploration score."""
        # TODO: Implement exploration score calculation
        return 0.0  # Placeholder
    
    def _generate_text_embedding(self, text: str) -> str:
        """Generate text embedding vector."""
        # TODO: Implement text embedding generation
        return "[]"  # Placeholder
    
    def _process_categories(self, categories: List) -> str:
        """Process categories into a vector."""
        # TODO: Implement category processing
        return "[]"  # Placeholder
    
    def _extract_topics(self, description: str) -> str:
        """Extract topics from description."""
        # TODO: Implement topic extraction
        return "[]"  # Placeholder
    
    def _normalize_price(self, price: float) -> float:
        """Normalize price value."""
        # TODO: Implement price normalization
        return 0.0  # Placeholder
    
    def _normalize_capacity(self, capacity: int) -> float:
        """Normalize capacity value."""
        # TODO: Implement capacity normalization
        return 0.0  # Placeholder
    
    def _extract_time_of_day(self, start_time: str) -> int:
        """Extract hour of day from start time."""
        # TODO: Implement time extraction
        return 0  # Placeholder
    
    def _extract_day_of_week(self, start_time: str) -> int:
        """Extract day of week from start time."""
        # TODO: Implement day extraction
        return 0  # Placeholder
    
    def _calculate_duration(self, start_time: str, end_time: str) -> float:
        """Calculate event duration in hours."""
        # TODO: Implement duration calculation
        return 0.0  # Placeholder
    
    def _calculate_attendance_score(self, event_data: Dict) -> float:
        """Calculate attendance score."""
        # TODO: Implement attendance score calculation
        return 0.0  # Placeholder
    
    def _calculate_social_score(self, event_data: Dict) -> float:
        """Calculate social score."""
        # TODO: Implement social score calculation
        return 0.0  # Placeholder
    
    def _calculate_organizer_rating(self, event_history: List) -> float:
        """Calculate a user's event organizing capability score.
        
        Args:
            event_history: List of events organized by the user with structure:
                [
                    {
                        'event_id': str,
                        'attendees': int,
                        'rating': float,  # Average rating 0-5
                        'reviews': int,   # Number of reviews
                        'complexity': str, # 'low', 'medium', 'high'
                        'duration_hours': float,
                        'budget': float,
                        'team_size': int,
                        'type': str,      # Event type
                        'success_metrics': {
                            'attendance_rate': float,
                            'satisfaction': float,
                            'budget_adherence': float,
                            'timeline_adherence': float
                        }
                    },
                    ...
                ]
                
        Returns:
            Float between 0 and 1 representing organizing capability
        """
        try:
            if not event_history:
                return 0.0
                
            # Define weights for different factors
            weights = {
                'event_rating': 0.25,
                'event_scale': 0.20,
                'complexity': 0.15,
                'success_metrics': 0.25,
                'experience': 0.15
            }
            
            # Initialize score components
            scores = {
                'event_rating': 0.0,
                'event_scale': 0.0,
                'complexity': 0.0,
                'success_metrics': 0.0,
                'experience': 0.0
            }
            
            # Calculate experience level based on number of events
            num_events = len(event_history)
            scores['experience'] = min(1.0, np.log1p(num_events) / np.log1p(20))  # Caps at 20 events
            
            # Process each event
            for event in event_history:
                # Event rating score (weighted by number of reviews)
                rating = event.get('rating', 0.0)
                reviews = event.get('reviews', 0)
                rating_weight = min(1.0, np.log1p(reviews) / np.log1p(50))  # Caps at 50 reviews
                scores['event_rating'] += (rating / 5.0) * rating_weight
                
                # Event scale score based on attendees and team size
                attendees = event.get('attendees', 0)
                team_size = event.get('team_size', 1)
                scale_score = min(1.0, np.log1p(attendees) / np.log1p(1000))  # Caps at 1000 attendees
                scale_score *= min(1.0, np.log1p(team_size) / np.log1p(10))   # Caps at team of 10
                scores['event_scale'] += scale_score
                
                # Complexity score
                complexity_levels = {'low': 0.3, 'medium': 0.6, 'high': 1.0}
                complexity = event.get('complexity', 'low').lower()
                scores['complexity'] += complexity_levels.get(complexity, 0.3)
                
                # Success metrics score
                metrics = event.get('success_metrics', {})
                if metrics:
                    metric_scores = [
                        metrics.get('attendance_rate', 0.0),
                        metrics.get('satisfaction', 0.0),
                        metrics.get('budget_adherence', 0.0),
                        metrics.get('timeline_adherence', 0.0)
                    ]
                    scores['success_metrics'] += sum(m for m in metric_scores if m > 0) / len(metric_scores)
            
            # Normalize accumulated scores by number of events
            for key in ['event_rating', 'event_scale', 'complexity', 'success_metrics']:
                scores[key] = scores[key] / num_events if num_events > 0 else 0.0
            
            # Calculate weighted final score
            final_score = sum(scores[k] * weights[k] for k in weights)
            
            # Apply bonus for consistent high performance
            if num_events >= 5 and scores['event_rating'] > 0.8 and scores['success_metrics'] > 0.8:
                final_score = min(1.0, final_score * 1.2)
            
            return final_score
            
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