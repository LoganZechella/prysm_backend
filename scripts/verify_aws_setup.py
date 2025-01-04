#!/usr/bin/env python3
import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
from dotenv import load_dotenv

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.aws import AWSConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_aws_credentials():
    """Verify AWS credentials are properly configured"""
    try:
        # Initialize boto3 client (it will use AWS CLI credentials automatically)
        region = 'us-west-2'
        s3 = boto3.client('s3', region_name=region)
        
        # Test S3 access
        bucket_name = 'prysm-events-test'
        logger.info(f"Testing access to S3 bucket: {bucket_name}")
        
        try:
            # First, list all buckets to verify basic S3 access
            response = s3.list_buckets()
            existing_buckets = [bucket['Name'] for bucket in response['Buckets']]
            logger.info(f"Found existing buckets: {existing_buckets}")
            
            if bucket_name in existing_buckets:
                logger.info(f"Successfully verified access to existing S3 bucket: {bucket_name}")
            else:
                logger.info(f"Bucket {bucket_name} does not exist yet. Attempting to create...")
                try:
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                    logger.info(f"Successfully created S3 bucket: {bucket_name}")
                except ClientError as create_error:
                    logger.error(f"Failed to create S3 bucket: {str(create_error)}")
                    return False
                    
            # Test DynamoDB access
            dynamodb = boto3.client('dynamodb', region_name=region)
            tables = dynamodb.list_tables()
            logger.info(f"Successfully accessed DynamoDB. Found {len(tables.get('TableNames', []))} tables.")
            
            logger.info("AWS credentials verification completed successfully!")
            return True
            
        except ClientError as e:
            logger.error(f"Error accessing AWS services: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Error during AWS verification: {str(e)}")
        return False

if __name__ == "__main__":
    success = verify_aws_credentials()
    sys.exit(0 if success else 1) 