#!/usr/bin/env python3
import asyncio
import logging
import os
import sys
import boto3
from botocore.exceptions import ClientError

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.storage import StorageConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_s3_bucket(bucket_name: str, region: str) -> bool:
    """Create S3 bucket with versioning enabled"""
    try:
        s3_client = boto3.client('s3', region_name=region)
        
        # Create bucket with region-specific configuration
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
        
        # Enable versioning
        s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        
        # Configure lifecycle rules
        lifecycle_config = {
            'Rules': [
                {
                    'ID': 'raw-data-retention',
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'raw/'
                    },
                    'Expiration': {
                        'Days': 90
                    }
                },
                {
                    'ID': 'processed-data-retention',
                    'Status': 'Enabled',
                    'Filter': {
                        'Prefix': 'processed/'
                    },
                    'Expiration': {
                        'Days': 30
                    }
                }
            ]
        }
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        
        logger.info(f"Created S3 bucket: {bucket_name}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.info(f"S3 bucket already exists: {bucket_name}")
            return True
        elif e.response['Error']['Code'] == 'BucketAlreadyExists':
            logger.error(f"S3 bucket name already taken: {bucket_name}")
            return False
        else:
            logger.error(f"Error creating S3 bucket: {str(e)}")
            return False

def create_dynamodb_table(table_name: str, region: str) -> bool:
    """Create DynamoDB table with required indexes"""
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        
        # Create table with event_id as hash key
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'event_id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'event_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'start_datetime',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'location_hash',
                    'AttributeType': 'S'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'start_datetime-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'start_datetime',
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                },
                {
                    'IndexName': 'location-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'location_hash',
                            'KeyType': 'HASH'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            },
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            }
        )
        
        # Wait for table to be created
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        
        logger.info(f"Created DynamoDB table: {table_name}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            logger.info(f"DynamoDB table already exists: {table_name}")
            return True
        else:
            logger.error(f"Error creating DynamoDB table: {str(e)}")
            return False

def main():
    """Main entry point"""
    try:
        # Load configuration
        config = StorageConfig()
        
        # Create S3 bucket
        s3_success = create_s3_bucket(
            config.get_s3_bucket_name(),
            config.get_s3_region()
        )
        
        # Create DynamoDB table
        dynamodb_success = create_dynamodb_table(
            config.get_dynamodb_table_name(),
            config.get_dynamodb_region()
        )
        
        if s3_success and dynamodb_success:
            logger.info("\nStorage infrastructure initialized successfully!")
            sys.exit(0)
        else:
            logger.error("\nFailed to initialize storage infrastructure!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"\nError initializing storage infrastructure: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 