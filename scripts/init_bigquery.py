#!/usr/bin/env python3
"""
Script to initialize BigQuery tables for the data ingestion pipeline.
This script should be run during deployment to set up the necessary tables.
"""

import os
import argparse
from google.cloud import bigquery
from config.bigquery_schemas import create_insights_tables

def create_dataset_if_not_exists(project_id: str, dataset_id: str, location: str = "US"):
    """Create a BigQuery dataset if it doesn't exist."""
    client = bigquery.Client()
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_ref} already exists")
    except Exception as e:
        if "Not found" in str(e):
            # Create the dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = location
            dataset = client.create_dataset(dataset)
            print(f"Created dataset {dataset_ref}")
        else:
            raise e

def main():
    parser = argparse.ArgumentParser(description="Initialize BigQuery tables for data ingestion pipeline")
    parser.add_argument("--project-id", required=True, help="Google Cloud project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")
    parser.add_argument("--location", default="US", help="Dataset location (default: US)")
    
    args = parser.parse_args()
    
    # Create dataset if it doesn't exist
    create_dataset_if_not_exists(args.project_id, args.dataset_id, args.location)
    
    # Create tables if they don't exist
    create_insights_tables(args.project_id, args.dataset_id)

if __name__ == "__main__":
    main() 