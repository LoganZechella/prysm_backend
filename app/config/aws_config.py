"""AWS configuration for SageMaker and other AWS services."""
import os
from typing import Dict, Any

# AWS Configuration
AWS_CONFIG = {
    'region_name': os.getenv('AWS_REGION', 'us-west-2'),  # Change to your preferred region
    'sagemaker_role_arn': os.getenv('AWS_SAGEMAKER_ROLE_ARN'),
    'feature_store_role_arn': os.getenv('AWS_FEATURE_STORE_ROLE_ARN'),
    's3_bucket': os.getenv('AWS_S3_BUCKET'),
    'feature_store_offline_prefix': 'feature-store',
    'model_artifact_prefix': 'models',
    'training_data_prefix': 'training-data'
}

def validate_aws_config() -> None:
    """Validate AWS configuration."""
    required_vars = [
        'AWS_REGION',
        'AWS_SAGEMAKER_ROLE_ARN',
        'AWS_FEATURE_STORE_ROLE_ARN',
        'AWS_S3_BUCKET'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(
            f"Missing required AWS environment variables: {', '.join(missing)}\n"
            "Please set these variables in your environment or .env file."
        ) 