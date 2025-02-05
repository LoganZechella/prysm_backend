"""Script to set up Amazon Personalize resources and import initial data."""
import os
import json
import boto3
import time
import asyncio
import logging
import csv
import pandas as pd
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.models.event import EventModel
from app.models.traits import Traits
from app.models.feedback import ImplicitFeedback, UserFeedback
from app.services.personalize_transformer import PersonalizeTransformer
from app.core.config import settings
from typing import Tuple, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema definitions for ECOMMERCE domain
USER_SCHEMA = {
    "type": "record",
    "name": "Users",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {"name": "USER_ID", "type": "string"},
        {"name": "GENDER", "type": ["null", "string"], "categorical": True},  # Required for ECOMMERCE domain
        {"name": "AGE_GROUP", "type": ["null", "string"], "categorical": True},  # Required for ECOMMERCE domain
        {"name": "INTERESTS", "type": ["null", "string"], "metadata": True},  # Custom metadata
        {"name": "PROFESSIONAL_FIELD", "type": ["null", "string"], "metadata": True},  # Custom metadata
        {"name": "BEHAVIOR_PATTERNS", "type": ["null", "string"], "metadata": True}  # Custom metadata
    ]
}

ITEM_SCHEMA = {
    "type": "record",
    "name": "Items",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {"name": "ITEM_ID", "type": "string"},
        {"name": "CATEGORY_L1", "type": "string", "categorical": True},  # Required for ECOMMERCE domain
        {"name": "CATEGORY_L2", "type": ["null", "string"], "categorical": True},  # Required for ECOMMERCE domain
        {"name": "PRICE", "type": "float"},  # Required for ECOMMERCE domain
        {"name": "DESCRIPTION", "type": "string", "textual": True},  # Required for ECOMMERCE domain
        {"name": "VENUE_CITY", "type": ["null", "string"], "categorical": True},  # Custom metadata
        {"name": "VENUE_STATE", "type": ["null", "string"], "categorical": True},  # Custom metadata
        {"name": "IS_ONLINE", "type": ["null", "boolean"], "metadata": True},  # Custom metadata
        {"name": "TECHNICAL_LEVEL", "type": ["null", "float"], "metadata": True},  # Custom metadata
        {"name": "CREATION_TIMESTAMP", "type": "long"}
    ]
}

INTERACTIONS_SCHEMA = {
    "type": "record",
    "name": "Interactions",
    "namespace": "com.amazonaws.personalize.schema",
    "fields": [
        {"name": "USER_ID", "type": "string"},
        {"name": "ITEM_ID", "type": "string"},
        {"name": "EVENT_TYPE", "type": "string"},
        {"name": "EVENT_VALUE", "type": ["null", "float"]},
        {"name": "TIMESTAMP", "type": "long"}
    ]
}

# Get the Personalize role ARN
def get_personalize_role_arn():
    """Get the ARN of the Personalize role."""
    iam = boto3.client('iam', region_name=settings.AWS_DEFAULT_REGION)
    try:
        response = iam.get_role(RoleName='prysm-personalize-role')
        return response['Role']['Arn']
    except Exception as e:
        logger.error(f"Error getting Personalize role ARN: {str(e)}")
        raise

async def export_users_to_csv(db: AsyncSession, output_file: str):
    """Export user data to CSV for Personalize."""
    logger.info("Exporting users to CSV...")
    transformer = PersonalizeTransformer(db)
    
    # Get all transformed user data
    users_data = await transformer.get_all_user_data()
    
    # Convert to pandas DataFrame for consistent handling
    df = pd.DataFrame(users_data)
    
    # Ensure all string columns are properly escaped
    string_columns = ['USER_ID', 'GENDER', 'AGE_GROUP', 'INTERESTS', 'PROFESSIONAL_FIELD', 'BEHAVIOR_PATTERNS']
    for col in string_columns:
        if col in df.columns:
            # Replace newlines and quotes
            df[col] = df[col].apply(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').replace('"', '""'))
            # Ensure no empty strings in required fields
            if col in ['USER_ID', 'GENDER', 'AGE_GROUP']:
                df[col] = df[col].replace('', 'Unknown')
    
    # Validate against schema requirements
    required_cols = ['USER_ID', 'GENDER', 'AGE_GROUP', 'INTERESTS', 'PROFESSIONAL_FIELD', 'BEHAVIOR_PATTERNS']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Write to CSV with proper quoting and escaping
    df.to_csv(
        output_file,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar='\\',
        doublequote=True,
        encoding='utf-8'
    )
    
    logger.info(f"Exported {len(users_data)} users to {output_file}")

async def export_events_to_csv(db: AsyncSession, output_file: str):
    """Export event data to CSV for Personalize."""
    logger.info("Exporting events to CSV...")
    transformer = PersonalizeTransformer(db)
    
    # Get all transformed event data
    events_data = await transformer.get_all_event_data()
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(events_data)
    
    # Ensure all string columns are properly escaped
    string_columns = ['ITEM_ID', 'CATEGORY_L1', 'CATEGORY_L2', 'DESCRIPTION', 'VENUE_CITY', 'VENUE_STATE']
    for col in string_columns:
        if col in df.columns:
            # Replace newlines and quotes
            df[col] = df[col].apply(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').replace('"', '""'))
            # Ensure no empty strings in required fields
            if col in ['ITEM_ID', 'CATEGORY_L1', 'CATEGORY_L2', 'DESCRIPTION']:
                df[col] = df[col].replace('', 'Unknown')
    
    # Ensure numeric columns are properly formatted
    df['PRICE'] = pd.to_numeric(df['PRICE'], errors='coerce').fillna(0.0)
    df['TECHNICAL_LEVEL'] = pd.to_numeric(df['TECHNICAL_LEVEL'], errors='coerce').fillna(0.0)
    df['CREATION_TIMESTAMP'] = pd.to_numeric(df['CREATION_TIMESTAMP'], errors='coerce').fillna(int(datetime.now().timestamp()))
    
    # Convert boolean column
    df['IS_ONLINE'] = df['IS_ONLINE'].fillna(False).astype(bool)
    
    # Validate against schema requirements
    required_cols = ['ITEM_ID', 'CATEGORY_L1', 'CATEGORY_L2', 'PRICE', 'DESCRIPTION', 
                    'VENUE_CITY', 'VENUE_STATE', 'IS_ONLINE', 'TECHNICAL_LEVEL', 'CREATION_TIMESTAMP']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Write to CSV with proper quoting and escaping
    df.to_csv(
        output_file,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,  # Quote all non-numeric fields
        escapechar='\\',  # Use backslash as escape character
        doublequote=True,  # Double up quotes for escaping
        encoding='utf-8'  # Ensure proper UTF-8 encoding
    )
    
    logger.info(f"Exported {len(events_data)} events to {output_file}")

async def export_interactions_to_csv(db: AsyncSession, output_file: str):
    """Export interaction data to CSV for Personalize."""
    logger.info("Exporting interactions to CSV...")
    transformer = PersonalizeTransformer(db)
    
    # Get all transformed interaction data
    interactions_data = await transformer.get_all_interaction_data()
    
    # Convert to pandas DataFrame for consistent handling
    df = pd.DataFrame(interactions_data)
    
    # Ensure string columns are properly escaped
    string_columns = ['USER_ID', 'ITEM_ID', 'EVENT_TYPE']
    for col in string_columns:
        if col in df.columns:
            # Replace newlines and quotes
            df[col] = df[col].apply(lambda x: str(x).replace('\n', ' ').replace('\r', ' ').replace('"', '""'))
            # Ensure no empty strings in required fields
            df[col] = df[col].replace('', 'Unknown')
    
    # Ensure numeric columns are properly formatted
    df['EVENT_VALUE'] = pd.to_numeric(df['EVENT_VALUE'], errors='coerce')  # Allow NaN for optional field
    df['TIMESTAMP'] = pd.to_numeric(df['TIMESTAMP'], errors='coerce').fillna(int(datetime.now().timestamp()))
    
    # Validate against schema requirements
    required_cols = ['USER_ID', 'ITEM_ID', 'EVENT_TYPE', 'TIMESTAMP']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Write to CSV with proper quoting and escaping
    df.to_csv(
        output_file,
        index=False,
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar='\\',
        doublequote=True,
        encoding='utf-8'
    )
    
    logger.info(f"Exported {len(interactions_data)} interactions to {output_file}")

def create_dataset_group(personalize_client, group_name: str) -> str:
    """Create a dataset group in Amazon Personalize.
    
    Args:
        personalize_client: Boto3 Personalize client
        group_name: Name for the dataset group
        
    Returns:
        str: ARN of the created dataset group
    """
    try:
        response = personalize_client.create_dataset_group(
            name=group_name,
            domain='ECOMMERCE'  # Using ECOMMERCE domain as it's closest to events
        )
        dataset_group_arn = response['datasetGroupArn']
        logger.info(f"Created dataset group: {dataset_group_arn}")
        
        # Wait for the dataset group to become active
        status = None
        max_time = time.time() + 3600  # 1 hour timeout
        while time.time() < max_time:
            describe_response = personalize_client.describe_dataset_group(
                datasetGroupArn=dataset_group_arn
            )
            status = describe_response["datasetGroup"]["status"]
            logger.info(f"Dataset group status: {status}")
            
            if status == "ACTIVE":
                return dataset_group_arn
            elif status == "CREATE FAILED":
                raise Exception(f"Failed to create dataset group: {describe_response['datasetGroup'].get('failureReason', 'Unknown reason')}")
            
            time.sleep(10)
        
        raise Exception("Timeout waiting for dataset group to become active")
        
    except personalize_client.exceptions.ResourceAlreadyExistsException:
        # If the group already exists, get its ARN
        response = personalize_client.list_dataset_groups()
        for group in response['datasetGroups']:
            if group['name'] == group_name and group['status'] == 'ACTIVE':
                logger.info(f"Using existing dataset group: {group['datasetGroupArn']}")
                return group['datasetGroupArn']
        raise Exception(f"Dataset group {group_name} exists but is not active")

def create_schema(personalize_client, schema_name: str, schema_file_path: str) -> str:
    """Create a schema in Amazon Personalize from a JSON file.
    
    Args:
        personalize_client: Boto3 Personalize client
        schema_name: Name for the schema
        schema_file_path: Path to the JSON schema file
        
    Returns:
        str: ARN of the created schema
    """
    try:
        # Load schema from file
        with open(schema_file_path, 'r') as f:
            schema = json.load(f)
        
        # Create schema in Personalize
        response = personalize_client.create_schema(
            name=schema_name,
            schema=json.dumps(schema)
        )
        schema_arn = response['schemaArn']
        logger.info(f"Created schema: {schema_arn}")
        return schema_arn
        
    except personalize_client.exceptions.ResourceAlreadyExistsException:
        # If schema exists, get its ARN
        response = personalize_client.list_schemas()
        for existing_schema in response['schemas']:
            if existing_schema['name'] == schema_name:
                logger.info(f"Using existing schema: {existing_schema['schemaArn']}")
                return existing_schema['schemaArn']
        raise Exception(f"Schema {schema_name} exists but could not be retrieved")

def create_dataset(personalize_client, dataset_group_arn: str, schema_arn: str, 
                  dataset_name: str, dataset_type: str) -> str:
    """Create a dataset in Amazon Personalize.
    
    Args:
        personalize_client: Boto3 Personalize client
        dataset_group_arn: ARN of the dataset group
        schema_arn: ARN of the schema
        dataset_name: Name for the dataset
        dataset_type: Type of dataset (INTERACTIONS, USERS, or ITEMS)
        
    Returns:
        str: ARN of the created dataset
    """
    try:
        response = personalize_client.create_dataset(
            name=dataset_name,
            schemaArn=schema_arn,
            datasetGroupArn=dataset_group_arn,
            datasetType=dataset_type
        )
        dataset_arn = response['datasetArn']
        logger.info(f"Created dataset: {dataset_arn}")
        
        # Wait for the dataset to become active
        status = None
        max_time = time.time() + 3600  # 1 hour timeout
        while time.time() < max_time:
            describe_response = personalize_client.describe_dataset(
                datasetArn=dataset_arn
            )
            status = describe_response["dataset"]["status"]
            logger.info(f"Dataset status: {status}")
            
            if status == "ACTIVE":
                return dataset_arn
            elif status == "CREATE FAILED":
                raise Exception(f"Failed to create dataset: {describe_response['dataset'].get('failureReason', 'Unknown reason')}")
            
            time.sleep(10)
        
        raise Exception("Timeout waiting for dataset to become active")
        
    except personalize_client.exceptions.ResourceAlreadyExistsException:
        # If dataset exists, get its ARN
        response = personalize_client.list_datasets(datasetGroupArn=dataset_group_arn)
        for existing_dataset in response['datasets']:
            if existing_dataset['name'] == dataset_name:
                logger.info(f"Using existing dataset: {existing_dataset['datasetArn']}")
                return existing_dataset['datasetArn']
        raise Exception(f"Dataset {dataset_name} exists but could not be retrieved")

def create_import_job(personalize_client, job_name: str, dataset_arn: str, 
                     data_location: str, role_arn: str) -> str:
    """Create a dataset import job in Amazon Personalize.
    
    Args:
        personalize_client: Boto3 Personalize client
        job_name: Name for the import job
        dataset_arn: ARN of the dataset
        data_location: S3 URI of the data file
        role_arn: ARN of the IAM role for Personalize
        
    Returns:
        str: ARN of the created import job
    """
    try:
        response = personalize_client.create_dataset_import_job(
            jobName=job_name,
            datasetArn=dataset_arn,
            dataSource={'dataLocation': data_location},
            roleArn=role_arn,
            importMode='FULL'
        )
        import_job_arn = response['datasetImportJobArn']
        logger.info(f"Created dataset import job: {import_job_arn}")
        
        # Wait for the import job to complete
        status = None
        max_time = time.time() + 3600  # 1 hour timeout
        while time.time() < max_time:
            describe_response = personalize_client.describe_dataset_import_job(
                datasetImportJobArn=import_job_arn
            )
            status = describe_response["datasetImportJob"]["status"]
            logger.info(f"Import job status: {status}")
            
            if status == "ACTIVE":
                return import_job_arn
            elif status == "CREATE FAILED":
                raise Exception(f"Failed to create import job: {describe_response['datasetImportJob'].get('failureReason', 'Unknown reason')}")
            
            time.sleep(10)
        
        raise Exception("Timeout waiting for import job to complete")
        
    except personalize_client.exceptions.ResourceAlreadyExistsException:
        raise Exception(f"Import job {job_name} already exists. Please use a different name.")

def set_s3_bucket_policy(bucket_name: str):
    """Set the bucket policy to allow Personalize access."""
    logger.info(f"Setting bucket policy for {bucket_name}...")
    s3 = boto3.client('s3', region_name=settings.AWS_DEFAULT_REGION)
    
    # Create bucket policy
    bucket_policy = {
        "Version": "2012-10-17",
        "Id": "PersonalizeS3BucketAccessPolicy",
        "Statement": [
            {
                "Sid": "PersonalizeS3BucketAccessPolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*"
                ]
            }
        ]
    }
    
    try:
        # Convert the policy to JSON string
        bucket_policy_string = json.dumps(bucket_policy)
        
        # Set the new policy
        s3.put_bucket_policy(
            Bucket=bucket_name,
            Policy=bucket_policy_string
        )
        logger.info(f"Successfully set bucket policy for {bucket_name}")
    except Exception as e:
        logger.error(f"Error setting bucket policy: {str(e)}")
        raise

def create_s3_bucket_if_not_exists(bucket_name: str) -> None:
    """Create an S3 bucket if it doesn't exist."""
    logger.info(f"Checking if bucket {bucket_name} exists...")
    s3 = boto3.client('s3', region_name=settings.AWS_DEFAULT_REGION)
    
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
    except s3.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404':
            logger.info(f"Creating bucket {bucket_name}...")
            try:
                s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': settings.AWS_DEFAULT_REGION
                    }
                )
                logger.info(f"Created bucket {bucket_name}")
            except Exception as e:
                logger.error(f"Error creating bucket: {str(e)}")
                raise
        else:
            logger.error(f"Error checking bucket: {str(e)}")
            raise
    
    # Set bucket policy
    set_s3_bucket_policy(bucket_name)

def upload_to_s3(local_path: str, bucket: str, key: str) -> str:
    """Upload a file to S3 and return its S3 location."""
    logger.info(f"Uploading {local_path} to S3...")
    s3 = boto3.client('s3', region_name=settings.AWS_DEFAULT_REGION)
    
    try:
        # Create bucket if it doesn't exist
        create_s3_bucket_if_not_exists(bucket)
        
        # Upload file
        s3.upload_file(local_path, bucket, key)
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        raise

def wait_for_import_job(personalize_client, import_job_arn: str):
    """Wait for an import job to complete."""
    logger.info(f"Waiting for import job {import_job_arn}...")
    
    while True:
        response = personalize_client.describe_dataset_import_job(
            datasetImportJobArn=import_job_arn
        )
        status = response['datasetImportJob']['status']
        logger.info(f"Import job status: {status}")
        
        if status == 'ACTIVE':
            break
        elif status in ['CREATE FAILED', 'FAILED']:
            raise Exception(f"Import job failed: {response['datasetImportJob'].get('failureReason', 'Unknown reason')}")
            
        time.sleep(60)  # Check every minute

def wait_for_solution_version(personalize_client, solution_version_arn: str):
    """Wait for a solution version to be created."""
    logger.info(f"Waiting for solution version {solution_version_arn}...")
    
    while True:
        response = personalize_client.describe_solution_version(
            solutionVersionArn=solution_version_arn
        )
        status = response['solutionVersion']['status']
        logger.info(f"Solution version status: {status}")
        
        if status == 'ACTIVE':
            break
        elif status in ['CREATE FAILED', 'FAILED']:
            raise Exception(f"Solution version failed: {response['solutionVersion'].get('failureReason', 'Unknown reason')}")
            
        time.sleep(60)  # Check every minute

def wait_for_campaign(personalize_client, campaign_arn: str):
    """Wait for a campaign to be created."""
    logger.info(f"Waiting for campaign {campaign_arn}...")
    
    while True:
        response = personalize_client.describe_campaign(
            campaignArn=campaign_arn
        )
        status = response['campaign']['status']
        logger.info(f"Campaign status: {status}")
        
        if status == 'ACTIVE':
            break
        elif status in ['CREATE FAILED', 'FAILED']:
            raise Exception(f"Campaign failed: {response['campaign'].get('failureReason', 'Unknown reason')}")
            
        time.sleep(60)  # Check every minute

def create_solution(
    personalize_client,
    dataset_group_arn: str,
    solution_name: str,
    recipe_arn: str
) -> Tuple[str, str]:
    """Create a solution and solution version."""
    logger.info(f"Creating solution: {solution_name}")
    
    # Create solution
    create_solution_response = personalize_client.create_solution(
        name=solution_name,
        datasetGroupArn=dataset_group_arn,
        recipeArn=recipe_arn
    )
    solution_arn = create_solution_response['solutionArn']
    
    # Create solution version
    create_solution_version_response = personalize_client.create_solution_version(
        solutionArn=solution_arn
    )
    solution_version_arn = create_solution_version_response['solutionVersionArn']
    
    # Wait for solution version to be ready
    wait_for_solution_version(personalize_client, solution_version_arn)
    
    return solution_arn, solution_version_arn

def create_campaign(
    personalize_client,
    campaign_name: str,
    solution_version_arn: str,
    min_provisioned_tps: int = 1
) -> str:
    """Create a campaign for serving recommendations."""
    logger.info(f"Creating campaign: {campaign_name}")
    
    response = personalize_client.create_campaign(
        name=campaign_name,
        solutionVersionArn=solution_version_arn,
        minProvisionedTPS=min_provisioned_tps
    )
    
    campaign_arn = response['campaignArn']
    
    # Wait for campaign to be ready
    wait_for_campaign(personalize_client, campaign_arn)
    
    return campaign_arn

def update_env_file(
    dataset_group_arn: str,
    recommender_arn: str,
    tracking_id: str
):
    """Update .env file with new ARNs."""
    logger.info("Updating .env file with new ARNs...")
    
    env_file = '.env'
    new_vars = {
        'AWS_PERSONALIZE_DATASET_GROUP_ARN': dataset_group_arn,
        'AWS_PERSONALIZE_RECOMMENDER_ARN': recommender_arn,
        'AWS_PERSONALIZE_TRACKING_ID': tracking_id
    }
    
    # Read existing .env file
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            lines = f.readlines()
    else:
        lines = []
    
    # Update or add new variables
    updated_vars = set()
    for i, line in enumerate(lines):
        for var_name in new_vars:
            if line.startswith(f'{var_name}='):
                lines[i] = f'{var_name}={new_vars[var_name]}\n'
                updated_vars.add(var_name)
    
    # Add any variables that weren't updated
    for var_name, value in new_vars.items():
        if var_name not in updated_vars:
            lines.append(f'{var_name}={value}\n')
    
    # Write back to .env file
    with open(env_file, 'w') as f:
        f.writelines(lines)
    
    logger.info("Updated .env file successfully")

def cleanup_personalize_resources():
    """Clean up existing Personalize resources."""
    logger.info("Cleaning up existing Personalize resources...")
    personalize = boto3.client('personalize', region_name=settings.AWS_DEFAULT_REGION)
    
    try:
        # List and delete schemas
        schemas = personalize.list_schemas()['schemas']
        for schema in schemas:
            if schema['name'].startswith('PrysmUsers') or schema['name'].startswith('PrysmItems') or schema['name'].startswith('PrysmInteractions'):
                logger.info(f"Deleting schema: {schema['name']}")
                try:
                    personalize.delete_schema(schemaArn=schema['schemaArn'])
                except Exception as e:
                    logger.error(f"Error deleting schema {schema['name']}: {str(e)}")
        
        # List dataset groups
        groups = personalize.list_dataset_groups()['datasetGroups']
        for group in groups:
            if group['name'].startswith('PrysmEvents'):
                logger.info(f"Processing dataset group: {group['name']}")
                group_arn = group['datasetGroupArn']
                
                # First, list and delete all datasets in the group
                try:
                    datasets = personalize.list_datasets(datasetGroupArn=group_arn)['datasets']
                    for dataset in datasets:
                        dataset_arn = dataset['datasetArn']
                        logger.info(f"Deleting dataset: {dataset['name']}")
                        try:
                            # Delete any import jobs first
                            paginator = personalize.get_paginator('list_dataset_import_jobs')
                            for page in paginator.paginate(datasetArn=dataset_arn):
                                for job in page['datasetImportJobs']:
                                    try:
                                        personalize.delete_dataset_import_job(
                                            datasetImportJobArn=job['datasetImportJobArn']
                                        )
                                        logger.info(f"Deleted import job: {job['datasetImportJobArn']}")
                                    except Exception as e:
                                        logger.error(f"Error deleting import job: {str(e)}")
                            
                            # Now delete the dataset
                            personalize.delete_dataset(datasetArn=dataset_arn)
                            logger.info(f"Successfully deleted dataset: {dataset['name']}")
                        except Exception as e:
                            logger.error(f"Error deleting dataset {dataset['name']}: {str(e)}")
                except Exception as e:
                    logger.error(f"Error listing datasets for group {group['name']}: {str(e)}")
                
                # Now try to delete the group
                try:
                    # Delete any recommenders
                    try:
                        recommenders = personalize.list_recommenders(datasetGroupArn=group_arn)['recommenders']
                        for recommender in recommenders:
                            try:
                                personalize.delete_recommender(recommenderArn=recommender['recommenderArn'])
                                logger.info(f"Deleted recommender: {recommender['name']}")
                            except Exception as e:
                                logger.error(f"Error deleting recommender {recommender['name']}: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error listing recommenders: {str(e)}")
                    
                    # Delete any event trackers
                    try:
                        trackers = personalize.list_event_trackers(datasetGroupArn=group_arn)['eventTrackers']
                        for tracker in trackers:
                            try:
                                personalize.delete_event_tracker(eventTrackerArn=tracker['eventTrackerArn'])
                                logger.info(f"Deleted event tracker: {tracker['name']}")
                            except Exception as e:
                                logger.error(f"Error deleting event tracker {tracker['name']}: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error listing event trackers: {str(e)}")
                    
                    # Finally delete the group
                    personalize.delete_dataset_group(datasetGroupArn=group_arn)
                    logger.info(f"Successfully deleted dataset group: {group['name']}")
                except Exception as e:
                    logger.error(f"Error deleting dataset group {group['name']}: {str(e)}")
        
        logger.info("Cleanup complete!")
        
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise

def wait_for_dataset(personalize_client, dataset_arn: str):
    """Wait for a dataset to be active."""
    logger.info(f"Waiting for dataset {dataset_arn}...")
    
    while True:
        response = personalize_client.describe_dataset(
            datasetArn=dataset_arn
        )
        status = response['dataset']['status']
        logger.info(f"Dataset status: {status}")
        
        if status == 'ACTIVE':
            break
        elif status in ['CREATE FAILED', 'FAILED']:
            raise Exception(f"Dataset failed: {response['dataset'].get('failureReason', 'Unknown reason')}")
            
        time.sleep(10)  # Check every 10 seconds

def cleanup_dataset_groups(personalize):
    """Clean up old dataset groups to stay within limits."""
    logger.info("Cleaning up old dataset groups...")
    
    # List all dataset groups
    response = personalize.list_dataset_groups()
    dataset_groups = response['datasetGroups']
    
    # Sort by creation time, keeping only the 3 most recent ones
    dataset_groups.sort(key=lambda x: x['creationDateTime'], reverse=True)
    groups_to_delete = dataset_groups[3:]
    
    for group in groups_to_delete:
        group_arn = group['datasetGroupArn']
        try:
            logger.info(f"Deleting dataset group: {group_arn}")
            personalize.delete_dataset_group(datasetGroupArn=group_arn)
        except Exception as e:
            logger.error(f"Error deleting dataset group {group_arn}: {str(e)}")

async def main():
    """Main function to set up Amazon Personalize resources."""
    try:
        # Initialize AWS clients
        personalize = boto3.client('personalize', region_name=settings.AWS_DEFAULT_REGION)
        
        # Generate timestamp for unique naming
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        # Step 1: Create Dataset Group
        logger.info("Step 1: Creating Dataset Group")
        dataset_group_name = f"PrysmEvents_{timestamp}"
        dataset_group_arn = create_dataset_group(personalize, dataset_group_name)
        logger.info(f"Dataset Group ARN: {dataset_group_arn}")
        
        # Step 2: Create Schema for Interactions
        logger.info("Step 2: Creating Schema for Interactions")
        schema_name = f"PrysmInteractions_{timestamp}"
        schema_file_path = "config/personalize_schemas/interactions_schema.json"
        schema_arn = create_schema(personalize, schema_name, schema_file_path)
        logger.info(f"Schema ARN: {schema_arn}")
        
        # Step 3: Create Dataset
        logger.info("Step 3: Creating Dataset")
        dataset_name = f"PrysmInteractionsDataset_{timestamp}"
        dataset_arn = create_dataset(
            personalize,
            dataset_group_arn,
            schema_arn,
            dataset_name,
            "INTERACTIONS"
        )
        logger.info(f"Dataset ARN: {dataset_arn}")
        
        # Step 4: Export interactions data to CSV
        logger.info("Step 4: Exporting interactions data")
        output_dir = "data/personalize"
        os.makedirs(output_dir, exist_ok=True)
        interactions_file = os.path.join(output_dir, f"interactions_{timestamp}.csv")
        
        async with AsyncSessionLocal() as db:
            await export_interactions_to_csv(db, interactions_file)
        
        # Step 5: Set up S3 bucket and upload data
        logger.info("Step 5: Setting up S3 and uploading data")
        bucket_name = settings.AWS_S3_BUCKET
        create_s3_bucket_if_not_exists(bucket_name)
        set_s3_bucket_policy(bucket_name)
        
        s3_key = f"personalize/interactions/interactions_{timestamp}.csv"
        s3_uri = upload_to_s3(interactions_file, bucket_name, s3_key)
        logger.info(f"Uploaded interactions data to: {s3_uri}")
        
        # Step 6: Create Import Job
        logger.info("Step 6: Creating Import Job")
        role_arn = get_personalize_role_arn()
        import_job_name = f"PrysmInteractionsImport_{timestamp}"
        import_job_arn = create_import_job(
            personalize,
            import_job_name,
            dataset_arn,
            s3_uri,
            role_arn
        )
        logger.info(f"Import Job ARN: {import_job_arn}")
        
        # Update environment variables with new ARNs
        logger.info("Updating environment variables")
        update_env_file(
            dataset_group_arn=dataset_group_arn,
            recommender_arn=None,  # Will be set after creating recommender
            tracking_id=None  # Will be set after creating event tracker
        )
        
        logger.info("Setup completed successfully!")
        return {
            "dataset_group_arn": dataset_group_arn,
            "schema_arn": schema_arn,
            "dataset_arn": dataset_arn,
            "import_job_arn": import_job_arn
        }
        
    except Exception as e:
        logger.error(f"Error during setup: {str(e)}")
        raise

def create_recommender(
    personalize_client,
    dataset_group_arn: str,
    recommender_name: str,
    recipe_arn: str,
    min_tps: int = 1
) -> str:
    """Create a recommender for domain dataset groups."""
    logger.info(f"Creating recommender: {recommender_name}")
    
    response = personalize_client.create_recommender(
        name=recommender_name,
        datasetGroupArn=dataset_group_arn,
        recipeArn=recipe_arn,
        description="Recommender for event recommendations",
        minRecommendationRequestsPerSecond=min_tps
    )
    
    recommender_arn = response['recommenderArn']
    
    # Wait for recommender to be active
    while True:
        response = personalize_client.describe_recommender(
            recommenderArn=recommender_arn
        )
        status = response['recommender']['status']
        logger.info(f"Recommender status: {status}")
        
        if status == 'ACTIVE':
            break
        elif status in ['CREATE FAILED', 'FAILED']:
            raise Exception(f"Recommender failed: {response['recommender'].get('failureReason', 'Unknown reason')}")
            
        time.sleep(60)  # Check every minute
    
    return recommender_arn

if __name__ == "__main__":
    asyncio.run(main()) 