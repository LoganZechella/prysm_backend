#!/usr/bin/env python3
import logging
import os
import sys
import time
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from typing import Dict, Any, List, Optional

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.storage import StorageConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_s3_metrics(bucket_name: str, region: str) -> Dict[str, Any]:
    """Get S3 bucket metrics"""
    try:
        s3_client = boto3.client('s3', region_name=region)
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        
        metrics = {
            'bucket_name': bucket_name,
            'region': region,
            'size': 0,
            'object_count': 0,
            'raw_objects': 0,
            'processed_objects': 0,
            'versioning_status': 'Unknown',
            'lifecycle_rules': []
        }
        
        # Get bucket versioning status
        versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
        metrics['versioning_status'] = versioning.get('Status', 'Disabled')
        
        # Get lifecycle rules
        lifecycle = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        metrics['lifecycle_rules'] = lifecycle.get('Rules', [])
        
        # Get storage metrics from CloudWatch
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=1)
        
        # Get bucket size
        size_response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName='BucketSizeBytes',
            Dimensions=[
                {'Name': 'BucketName', 'Value': bucket_name},
                {'Name': 'StorageType', 'Value': 'StandardStorage'}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Average']
        )
        
        if size_response['Datapoints']:
            metrics['size'] = size_response['Datapoints'][0]['Average']
        
        # Get object count
        count_response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName='NumberOfObjects',
            Dimensions=[
                {'Name': 'BucketName', 'Value': bucket_name},
                {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Average']
        )
        
        if count_response['Datapoints']:
            metrics['object_count'] = count_response['Datapoints'][0]['Average']
        
        # Count objects by prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        
        # Count raw objects
        raw_pages = paginator.paginate(Bucket=bucket_name, Prefix='raw/')
        for page in raw_pages:
            if 'Contents' in page:
                metrics['raw_objects'] += len(page['Contents'])
        
        # Count processed objects
        processed_pages = paginator.paginate(Bucket=bucket_name, Prefix='processed/')
        for page in processed_pages:
            if 'Contents' in page:
                metrics['processed_objects'] += len(page['Contents'])
        
        return metrics
        
    except ClientError as e:
        logger.error(f"Error getting S3 metrics: {str(e)}")
        return {}

def get_dynamodb_metrics(table_name: str, region: str) -> Dict[str, Any]:
    """Get DynamoDB table metrics"""
    try:
        dynamodb = boto3.client('dynamodb', region_name=region)
        cloudwatch = boto3.client('cloudwatch', region_name=region)
        
        metrics = {
            'table_name': table_name,
            'region': region,
            'item_count': 0,
            'table_size_bytes': 0,
            'read_capacity_units': 0,
            'write_capacity_units': 0,
            'read_throttle_events': 0,
            'write_throttle_events': 0,
            'indexes': []
        }
        
        # Get table description
        table = dynamodb.describe_table(TableName=table_name)
        table_info = table['Table']
        
        metrics['item_count'] = table_info.get('ItemCount', 0)
        metrics['table_size_bytes'] = table_info.get('TableSizeBytes', 0)
        
        # Get provisioned capacity
        if 'ProvisionedThroughput' in table_info:
            metrics['read_capacity_units'] = table_info['ProvisionedThroughput']['ReadCapacityUnits']
            metrics['write_capacity_units'] = table_info['ProvisionedThroughput']['WriteCapacityUnits']
        
        # Get index information
        if 'GlobalSecondaryIndexes' in table_info:
            metrics['indexes'] = table_info['GlobalSecondaryIndexes']
        
        # Get CloudWatch metrics
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=1)
        
        # Get throttle events
        read_throttle = cloudwatch.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='ReadThrottleEvents',
            Dimensions=[{'Name': 'TableName', 'Value': table_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        
        write_throttle = cloudwatch.get_metric_statistics(
            Namespace='AWS/DynamoDB',
            MetricName='WriteThrottleEvents',
            Dimensions=[{'Name': 'TableName', 'Value': table_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=['Sum']
        )
        
        if read_throttle['Datapoints']:
            metrics['read_throttle_events'] = read_throttle['Datapoints'][0]['Sum']
        if write_throttle['Datapoints']:
            metrics['write_throttle_events'] = write_throttle['Datapoints'][0]['Sum']
        
        return metrics
        
    except ClientError as e:
        logger.error(f"Error getting DynamoDB metrics: {str(e)}")
        return {}

def monitor_storage(interval: int = 300) -> None:
    """Monitor storage infrastructure continuously"""
    try:
        config = StorageConfig()
        
        while True:
            logger.info("\n=== Storage Infrastructure Monitoring ===")
            logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
            
            # Get S3 metrics
            s3_metrics = get_s3_metrics(
                config.get_s3_bucket_name(),
                config.get_s3_region()
            )
            
            if s3_metrics:
                logger.info("\nS3 Bucket Metrics:")
                logger.info(f"Bucket: {s3_metrics['bucket_name']}")
                logger.info(f"Size: {s3_metrics['size'] / (1024 * 1024 * 1024):.2f} GB")
                logger.info(f"Total Objects: {s3_metrics['object_count']}")
                logger.info(f"Raw Objects: {s3_metrics['raw_objects']}")
                logger.info(f"Processed Objects: {s3_metrics['processed_objects']}")
                logger.info(f"Versioning: {s3_metrics['versioning_status']}")
            
            # Get DynamoDB metrics
            dynamodb_metrics = get_dynamodb_metrics(
                config.get_dynamodb_table_name(),
                config.get_dynamodb_region()
            )
            
            if dynamodb_metrics:
                logger.info("\nDynamoDB Table Metrics:")
                logger.info(f"Table: {dynamodb_metrics['table_name']}")
                logger.info(f"Items: {dynamodb_metrics['item_count']}")
                logger.info(f"Size: {dynamodb_metrics['table_size_bytes'] / (1024 * 1024):.2f} MB")
                logger.info(f"Read Capacity: {dynamodb_metrics['read_capacity_units']}")
                logger.info(f"Write Capacity: {dynamodb_metrics['write_capacity_units']}")
                logger.info(f"Read Throttle Events: {dynamodb_metrics['read_throttle_events']}")
                logger.info(f"Write Throttle Events: {dynamodb_metrics['write_throttle_events']}")
                logger.info(f"Number of Indexes: {len(dynamodb_metrics['indexes'])}")
            
            logger.info("\n" + "=" * 40 + "\n")
            
            # Wait for next interval
            time.sleep(interval)
            
    except KeyboardInterrupt:
        logger.info("\nMonitoring stopped by user")
    except Exception as e:
        logger.error(f"Error during monitoring: {str(e)}")
        sys.exit(1)

def main():
    """Main entry point"""
    try:
        # Start monitoring with 5-minute interval
        monitor_storage(300)
    except Exception as e:
        logger.error(f"Error starting monitoring: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 