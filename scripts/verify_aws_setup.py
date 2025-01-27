#!/usr/bin/env python3
import os
import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import logging
from dotenv import load_dotenv
import sagemaker
from app.config.aws_config import AWS_CONFIG, validate_aws_config

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.aws import AWSConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_iam_role():
    """Verify IAM role exists and has correct permissions."""
    iam = boto3.client('iam')
    try:
        role_name = AWS_CONFIG['sagemaker_role_arn'].split('/')[-1]
        response = iam.get_role(RoleName=role_name)
        print(f"✅ IAM Role '{role_name}' exists")
        
        # Check attached policies
        policies = iam.list_attached_role_policies(RoleName=role_name)
        required_policies = [
            'AmazonSageMakerFullAccess',
            'AmazonS3FullAccess',
            'AmazonFeatureStoreAccess'
        ]
        
        attached_policies = [p['PolicyName'] for p in policies['AttachedPolicies']]
        missing_policies = set(required_policies) - set(attached_policies)
        
        if missing_policies:
            print(f"❌ Missing required policies: {missing_policies}")
        else:
            print("✅ All required policies are attached")
            
    except ClientError as e:
        print(f"❌ Error verifying IAM role: {str(e)}")

def verify_s3_bucket():
    """Verify S3 bucket exists and is accessible."""
    s3 = boto3.client('s3')
    try:
        s3.head_bucket(Bucket=AWS_CONFIG['s3_bucket'])
        print(f"✅ S3 bucket '{AWS_CONFIG['s3_bucket']}' exists and is accessible")
    except ClientError as e:
        print(f"❌ Error verifying S3 bucket: {str(e)}")

def verify_feature_store():
    """Verify Feature Store setup."""
    feature_store = boto3.client('sagemaker-featurestore-runtime')
    try:
        # Try to describe feature groups
        sagemaker_client = boto3.client('sagemaker')
        response = sagemaker_client.list_feature_groups()
        
        required_groups = ['user-features', 'event-features']
        existing_groups = [g['FeatureGroupName'] for g in response['FeatureGroupSummaries']]
        
        for group in required_groups:
            if group in existing_groups:
                print(f"✅ Feature group '{group}' exists")
            else:
                print(f"❌ Feature group '{group}' not found")
                
    except ClientError as e:
        print(f"❌ Error verifying Feature Store: {str(e)}")

def verify_sagemaker_access():
    """Verify SageMaker access."""
    try:
        sagemaker_session = sagemaker.Session()
        print(f"✅ Successfully created SageMaker session")
        
        # Try to list training jobs
        client = boto3.client('sagemaker')
        response = client.list_training_jobs(MaxResults=1)
        print("✅ Successfully connected to SageMaker API")
        
    except Exception as e:
        print(f"❌ Error verifying SageMaker access: {str(e)}")

def main():
    """Run all verifications."""
    print("\n🔍 Verifying AWS Setup...\n")
    
    try:
        # Validate config
        validate_aws_config()
        print("✅ AWS configuration is valid")
    except ValueError as e:
        print(f"❌ AWS configuration error: {str(e)}")
        return
    
    # Run verifications
    verify_iam_role()
    verify_s3_bucket()
    verify_feature_store()
    verify_sagemaker_access()
    
    print("\n✨ AWS setup verification complete!")

if __name__ == "__main__":
    main() 