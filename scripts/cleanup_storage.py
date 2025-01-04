#!/usr/bin/env python3
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

def delete_s3_bucket(bucket_name: str, region: str) -> bool:
    """Delete S3 bucket and all its contents"""
    try:
        s3_client = boto3.client('s3', region_name=region)
        
        # Delete all objects and versions
        paginator = s3_client.get_paginator('list_object_versions')
        try:
            for page in paginator.paginate(Bucket=bucket_name):
                objects = []
                
                # Add objects
                if 'Versions' in page:
                    for version in page['Versions']:
                        objects.append({
                            'Key': version['Key'],
                            'VersionId': version['VersionId']
                        })
                
                # Add delete markers
                if 'DeleteMarkers' in page:
                    for marker in page['DeleteMarkers']:
                        objects.append({
                            'Key': marker['Key'],
                            'VersionId': marker['VersionId']
                        })
                
                if objects:
                    s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects}
                    )
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchBucket':
                raise
        
        # Delete bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        logger.info(f"Deleted S3 bucket: {bucket_name}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.info(f"S3 bucket does not exist: {bucket_name}")
            return True
        else:
            logger.error(f"Error deleting S3 bucket: {str(e)}")
            return False

def delete_dynamodb_table(table_name: str, region: str) -> bool:
    """Delete DynamoDB table"""
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        
        # Delete table
        dynamodb.delete_table(TableName=table_name)
        
        # Wait for table to be deleted
        waiter = dynamodb.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        
        logger.info(f"Deleted DynamoDB table: {table_name}")
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.info(f"DynamoDB table does not exist: {table_name}")
            return True
        else:
            logger.error(f"Error deleting DynamoDB table: {str(e)}")
            return False

def main():
    """Main entry point"""
    try:
        # Load configuration
        config = StorageConfig()
        
        # Delete S3 bucket
        s3_success = delete_s3_bucket(
            config.get_s3_bucket_name(),
            config.get_s3_region()
        )
        
        # Delete DynamoDB table
        dynamodb_success = delete_dynamodb_table(
            config.get_dynamodb_table_name(),
            config.get_dynamodb_region()
        )
        
        if s3_success and dynamodb_success:
            logger.info("\nStorage infrastructure cleaned up successfully!")
            sys.exit(0)
        else:
            logger.error("\nFailed to clean up storage infrastructure!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"\nError cleaning up storage infrastructure: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 