"""Feature processor for SageMaker Feature Store integration."""
from typing import Dict, Any, List, Optional
import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
from datetime import datetime
import numpy as np
import logging
from pathlib import Path

from app.recommendation.features.user_traits import UserTraitProcessor
from app.recommendation.features.event_traits import EventFeatureProcessor
from app.monitoring.performance import PerformanceMonitor

logger = logging.getLogger(__name__)

class FeatureProcessor:
    """Processes features for SageMaker Feature Store."""
    
    def __init__(
        self,
        user_processor: UserTraitProcessor,
        event_processor: EventFeatureProcessor,
        region_name: str,
        feature_store_role_arn: str
    ):
        """Initialize the feature processor.
        
        Args:
            user_processor: Processor for user features
            event_processor: Processor for event features
            region_name: AWS region name
            feature_store_role_arn: ARN for Feature Store IAM role
        """
        self.user_processor = user_processor
        self.event_processor = event_processor
        self.performance_monitor = PerformanceMonitor()
        
        # Initialize AWS clients
        self.sagemaker_session = sagemaker.Session()
        self.feature_store_session = sagemaker.Session(
            boto_session=boto3.Session(region_name=region_name)
        )
        
        self.role_arn = feature_store_role_arn
        
        # Initialize feature groups
        self.user_feature_group = self._create_user_feature_group()
        self.event_feature_group = self._create_event_feature_group()
    
    def _create_user_feature_group(self) -> FeatureGroup:
        """Create or get user feature group."""
        feature_group = FeatureGroup(
            name="user-features",
            sagemaker_session=self.feature_store_session
        )
        
        try:
            feature_group.load_feature_definitions()
        except:
            # Create new feature group if it doesn't exist
            feature_group.feature_definitions = [
                {
                    "FeatureName": "user_id",
                    "FeatureType": "String"
                },
                {
                    "FeatureName": "genre_vector",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "artist_diversity",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "music_recency",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "music_consistency",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "social_engagement",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "collaboration_score",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "discovery_score",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "professional_interests",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "skill_vector",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "industry_vector",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "activity_times",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "engagement_level",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "exploration_score",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "event_time",
                    "FeatureType": "String"
                }
            ]
            
            feature_group.create(
                s3_uri=f"s3://{self.sagemaker_session.default_bucket()}/feature-store/user-features",
                record_identifier_name="user_id",
                event_time_feature_name="event_time",
                role_arn=self.role_arn,
                enable_online_store=True
            )
        
        return feature_group
    
    def _create_event_feature_group(self) -> FeatureGroup:
        """Create or get event feature group."""
        feature_group = FeatureGroup(
            name="event-features",
            sagemaker_session=self.feature_store_session
        )
        
        try:
            feature_group.load_feature_definitions()
        except:
            # Create new feature group if it doesn't exist
            feature_group.feature_definitions = [
                {
                    "FeatureName": "event_id",
                    "FeatureType": "String"
                },
                {
                    "FeatureName": "title_embedding",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "description_embedding",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "category_vector",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "topic_vector",
                    "FeatureType": "String"  # Store as JSON string
                },
                {
                    "FeatureName": "price_normalized",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "capacity_normalized",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "time_of_day",
                    "FeatureType": "Integer"
                },
                {
                    "FeatureName": "day_of_week",
                    "FeatureType": "Integer"
                },
                {
                    "FeatureName": "duration_hours",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "attendance_score",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "social_score",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "organizer_rating",
                    "FeatureType": "Float"
                },
                {
                    "FeatureName": "event_time",
                    "FeatureType": "String"
                }
            ]
            
            feature_group.create(
                s3_uri=f"s3://{self.sagemaker_session.default_bucket()}/feature-store/event-features",
                record_identifier_name="event_id",
                event_time_feature_name="event_time",
                role_arn=self.role_arn,
                enable_online_store=True
            )
        
        return feature_group
    
    async def process_and_store_user_features(
        self,
        user_id: str,
        user_data: Dict[str, Any]
    ) -> None:
        """Process user features and store in Feature Store.
        
        Args:
            user_id: User identifier
            user_data: Raw user data
        """
        try:
            with self.performance_monitor.monitor_operation('process_user_features'):
                # Process features using existing processor
                features = await self.user_processor.process(user_data)
                
                # Convert numpy arrays to lists for JSON serialization
                record = {
                    "user_id": user_id,
                    "genre_vector": features['genre_vector'].tolist(),
                    "artist_diversity": float(features['artist_diversity']),
                    "music_recency": float(features['music_recency']),
                    "music_consistency": float(features['music_consistency']),
                    "social_engagement": float(features['social_engagement']),
                    "collaboration_score": float(features['collaboration_score']),
                    "discovery_score": float(features['discovery_score']),
                    "professional_interests": features['professional_interests'],
                    "skill_vector": features['skill_vector'].tolist(),
                    "industry_vector": features['industry_vector'].tolist(),
                    "activity_times": features['activity_times'],
                    "engagement_level": float(features['engagement_level']),
                    "exploration_score": float(features['exploration_score']),
                    "event_time": datetime.utcnow().isoformat()
                }
                
                # Ingest into Feature Store
                self.user_feature_group.ingest(
                    data_frame=[record],
                    max_workers=1,
                    wait=True
                )
                
        except Exception as e:
            logger.error(f"Error processing user features: {str(e)}")
            raise
    
    async def process_and_store_event_features(
        self,
        event_id: str,
        event_data: Dict[str, Any]
    ) -> None:
        """Process event features and store in Feature Store.
        
        Args:
            event_id: Event identifier
            event_data: Raw event data
        """
        try:
            with self.performance_monitor.monitor_operation('process_event_features'):
                # Process features using existing processor
                features = await self.event_processor.process(event_data)
                
                # Convert numpy arrays to lists for JSON serialization
                record = {
                    "event_id": event_id,
                    "title_embedding": features['title_embedding'].tolist(),
                    "description_embedding": features['description_embedding'].tolist(),
                    "category_vector": features['category_vector'].tolist(),
                    "topic_vector": features['topic_vector'].tolist(),
                    "price_normalized": float(features['price_normalized']),
                    "capacity_normalized": float(features['capacity_normalized']),
                    "time_of_day": int(features['time_of_day']),
                    "day_of_week": int(features['day_of_week']),
                    "duration_hours": float(features['duration_hours']),
                    "attendance_score": float(features['attendance_score']),
                    "social_score": float(features['social_score']),
                    "organizer_rating": float(features['organizer_rating']),
                    "event_time": datetime.utcnow().isoformat()
                }
                
                # Ingest into Feature Store
                self.event_feature_group.ingest(
                    data_frame=[record],
                    max_workers=1,
                    wait=True
                )
                
        except Exception as e:
            logger.error(f"Error processing event features: {str(e)}")
            raise
    
    def get_training_data_query(self) -> str:
        """Get Athena query for joining user and event features.
        
        Returns:
            SQL query string for Feature Store
        """
        return f"""
        SELECT 
            u.*,
            e.*
        FROM 
            {self.user_feature_group.name} u
        CROSS JOIN 
            {self.event_feature_group.name} e
        WHERE 
            u.event_time >= NOW() - INTERVAL '30' DAY
        """ 