"""Example script for running the recommendation training pipeline."""
import os
import argparse
from typing import Dict, Any

from pipeline import RecommendationPipeline

def get_default_hyperparameters() -> Dict[str, Any]:
    """Get default hyperparameters for model training."""
    return {
        "epochs": 10,
        "batch_size": 64,
        "learning_rate": 0.001,
        "embedding_dim": 128,
        "hidden_dim": 256,
        "dropout": 0.2
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--feature-store-uri",
        type=str,
        required=True,
        help="URI of feature store data"
    )
    parser.add_argument(
        "--pipeline-name",
        type=str,
        default="recommendation-training",
        help="Name of the pipeline"
    )
    parser.add_argument(
        "--role-arn",
        type=str,
        help="ARN of SageMaker execution role"
    )
    args = parser.parse_args()
    
    # Create and build pipeline
    pipeline = RecommendationPipeline(
        role=args.role_arn,
        pipeline_name=args.pipeline_name
    )
    
    pipeline_definition = pipeline.build(
        feature_store_uri=args.feature_store_uri,
        hyperparameters=get_default_hyperparameters()
    )
    
    # Create/update pipeline
    pipeline_definition.upsert()
    
    # Start pipeline execution
    execution = pipeline_definition.start()
    print(f"Started pipeline execution: {execution.arn}")

if __name__ == "__main__":
    main() 