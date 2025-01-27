"""Script to set up SageMaker Feature Groups."""
import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.feature_store.feature_definition import FeatureDefinition, FeatureTypeEnum
from sagemaker.session import Session
import logging
from app.config.aws_config import AWS_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up AWS clients with explicit region
region = 'us-east-2'  # Match S3 bucket region
sagemaker_client = boto3.client('sagemaker', region_name=region)
s3_client = boto3.client('s3', region_name=region)

def create_user_feature_group(sagemaker_session):
    """Create user features group."""
    user_fg = FeatureGroup(
        name='user-features',
        sagemaker_session=sagemaker_session
    )
    
    user_features = [
        # Identifier
        FeatureDefinition(feature_name='user_id', feature_type=FeatureTypeEnum.STRING),
        
        # Music preferences
        FeatureDefinition(feature_name='genre_vector', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='artist_diversity', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='music_recency', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='music_consistency', feature_type=FeatureTypeEnum.FRACTIONAL),
        
        # Social metrics
        FeatureDefinition(feature_name='social_engagement', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='collaboration_score', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='discovery_score', feature_type=FeatureTypeEnum.FRACTIONAL),
        
        # Professional traits
        FeatureDefinition(feature_name='professional_interests', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='skill_vector', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='industry_vector', feature_type=FeatureTypeEnum.STRING),
        
        # Activity patterns
        FeatureDefinition(feature_name='activity_times', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='engagement_level', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='exploration_score', feature_type=FeatureTypeEnum.FRACTIONAL),
        
        # Required timestamp
        FeatureDefinition(feature_name='event_time', feature_type=FeatureTypeEnum.STRING)
    ]
    
    user_fg.feature_definitions = user_features
    
    try:
        logger.info("Creating user feature group...")
        user_fg.create(
            s3_uri=f"s3://{AWS_CONFIG['s3_bucket']}/feature-store/user-features",
            record_identifier_name="user_id",
            event_time_feature_name="event_time",
            role_arn=AWS_CONFIG['sagemaker_role_arn'],
            enable_online_store=True,
            description="User features for recommendation system"
        )
        logger.info("User feature group created successfully")
    except Exception as e:
        if "Resource Already Exists" in str(e):
            logger.info("User feature group already exists")
        else:
            raise
    
    return user_fg

def create_event_feature_group(sagemaker_session):
    """Create event features group."""
    event_fg = FeatureGroup(
        name='event-features',
        sagemaker_session=sagemaker_session
    )
    
    event_features = [
        # Identifier
        FeatureDefinition(feature_name='event_id', feature_type=FeatureTypeEnum.STRING),
        
        # Content embeddings
        FeatureDefinition(feature_name='title_embedding', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='description_embedding', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='category_vector', feature_type=FeatureTypeEnum.STRING),
        FeatureDefinition(feature_name='topic_vector', feature_type=FeatureTypeEnum.STRING),
        
        # Event attributes
        FeatureDefinition(feature_name='price_normalized', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='capacity_normalized', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='time_of_day', feature_type=FeatureTypeEnum.INTEGRAL),
        FeatureDefinition(feature_name='day_of_week', feature_type=FeatureTypeEnum.INTEGRAL),
        FeatureDefinition(feature_name='duration_hours', feature_type=FeatureTypeEnum.FRACTIONAL),
        
        # Engagement metrics
        FeatureDefinition(feature_name='attendance_score', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='social_score', feature_type=FeatureTypeEnum.FRACTIONAL),
        FeatureDefinition(feature_name='organizer_rating', feature_type=FeatureTypeEnum.FRACTIONAL),
        
        # Required timestamp
        FeatureDefinition(feature_name='event_time', feature_type=FeatureTypeEnum.STRING)
    ]
    
    event_fg.feature_definitions = event_features
    
    try:
        logger.info("Creating event feature group...")
        event_fg.create(
            s3_uri=f"s3://{AWS_CONFIG['s3_bucket']}/feature-store/event-features",
            record_identifier_name="event_id",
            event_time_feature_name="event_time",
            role_arn=AWS_CONFIG['sagemaker_role_arn'],
            enable_online_store=True,
            description="Event features for recommendation system"
        )
        logger.info("Event feature group created successfully")
    except Exception as e:
        if "Resource Already Exists" in str(e):
            logger.info("Event feature group already exists")
        else:
            raise
    
    return event_fg

def main():
    """Set up feature groups."""
    print("\n🔧 Setting up SageMaker Feature Groups...\n")
    
    try:
        # Initialize SageMaker session with explicit region
        boto_session = boto3.Session(region_name=region)
        sagemaker_session = Session(
            boto_session=boto_session,
            sagemaker_client=sagemaker_client
        )
        
        # Create feature groups
        user_fg = create_user_feature_group(sagemaker_session)
        event_fg = create_event_feature_group(sagemaker_session)
        
        print("\n✨ Feature Groups setup complete!")
        
        # Print feature group info
        print("\nFeature Groups Created:")
        print(f"- User Features Group: {user_fg.name}")
        print(f"- Event Features Group: {event_fg.name}")
        print("\nFeature groups are ready for use!")
        
    except Exception as e:
        print(f"\n❌ Error setting up feature groups: {str(e)}")
        raise

if __name__ == "__main__":
    main() 