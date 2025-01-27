"""Data Wrangler flow integration for preprocessing."""
from typing import Dict, Any, List, Optional
import boto3
import sagemaker
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.workflow.steps import ProcessingStep
from datetime import datetime
import logging
from pathlib import Path
import json

from app.monitoring.performance import PerformanceMonitor

logger = logging.getLogger(__name__)

class DataWranglerFlow:
    """Manages Data Wrangler flows for data preprocessing."""
    
    def __init__(
        self,
        region_name: str,
        role_arn: str,
        flow_source: str = "recommendation-flow"
    ):
        """Initialize the Data Wrangler flow.
        
        Args:
            region_name: AWS region name
            role_arn: ARN for SageMaker execution role
            flow_source: Name of the Data Wrangler flow source
        """
        self.performance_monitor = PerformanceMonitor()
        self.flow_source = flow_source
        self.role_arn = role_arn
        
        # Initialize AWS clients
        self.sagemaker_session = sagemaker.Session(
            boto_session=boto3.Session(region_name=region_name)
        )
        
        self.s3_client = boto3.client('s3')
    
    def create_processing_step(
        self,
        input_data_uri: str,
        output_data_uri: str,
        flow_name: str,
        instance_type: str = "ml.m5.xlarge",
        instance_count: int = 1
    ) -> ProcessingStep:
        """Create a processing step for the Data Wrangler flow.
        
        Args:
            input_data_uri: S3 URI for input data
            output_data_uri: S3 URI for output data
            flow_name: Name of the flow
            instance_type: SageMaker instance type
            instance_count: Number of instances
            
        Returns:
            SageMaker ProcessingStep
        """
        try:
            with self.performance_monitor.monitor_operation('create_processing_step'):
                # Create processing job
                flow_processor = sagemaker.processing.Processor(
                    role=self.role_arn,
                    instance_count=instance_count,
                    instance_type=instance_type,
                    sagemaker_session=self.sagemaker_session,
                    image_uri=self._get_data_wrangler_image_uri()
                )
                
                # Define inputs and outputs
                inputs = [
                    ProcessingInput(
                        source=input_data_uri,
                        destination="/opt/ml/processing/input"
                    )
                ]
                
                outputs = [
                    ProcessingOutput(
                        output_name="train",
                        source="/opt/ml/processing/output/train",
                        destination=f"{output_data_uri}/train"
                    ),
                    ProcessingOutput(
                        output_name="validation",
                        source="/opt/ml/processing/output/validation",
                        destination=f"{output_data_uri}/validation"
                    )
                ]
                
                # Create processing step
                return ProcessingStep(
                    name=f"DataWrangler-{flow_name}",
                    processor=flow_processor,
                    inputs=inputs,
                    outputs=outputs,
                    job_arguments=[
                        "--flow-source", self.flow_source,
                        "--flow-name", flow_name,
                        "--output-config", json.dumps({
                            "train_ratio": 0.8,
                            "validation_ratio": 0.2
                        })
                    ]
                )
                
        except Exception as e:
            logger.error(f"Error creating processing step: {str(e)}")
            raise
    
    def _get_data_wrangler_image_uri(self) -> str:
        """Get the Data Wrangler image URI for the current region."""
        return f"{self.sagemaker_session.boto_region_name}.amazonaws.com/sagemaker-data-wrangler-container:latest"
    
    def export_flow(
        self,
        flow_name: str,
        output_path: str
    ) -> None:
        """Export Data Wrangler flow to a Python script.
        
        Args:
            flow_name: Name of the flow to export
            output_path: Path to save the exported script
        """
        try:
            # Get flow export
            response = self.sagemaker_session.sagemaker_client.create_data_wrangler_flow_export(
                FlowSource=self.flow_source,
                FlowName=flow_name,
                OutputConfig={
                    "Format": "PYTHON_SCRIPT"
                }
            )
            
            # Save to file
            with open(output_path, 'w') as f:
                f.write(response['ExportedScript'])
                
        except Exception as e:
            logger.error(f"Error exporting flow: {str(e)}")
            raise
    
    def create_feature_transformation_flow(
        self,
        flow_name: str = "feature-transformation"
    ) -> None:
        """Create a Data Wrangler flow for feature transformation.
        
        Args:
            flow_name: Name of the flow to create
        """
        try:
            # Define flow configuration
            flow_config = {
                "nodes": [
                    {
                        "node_id": "source",
                        "type": "SOURCE",
                        "parameters": {
                            "dataset_definition": {
                                "data_distribution_type": "FullyReplicated",
                                "input_mode": "File"
                            }
                        }
                    },
                    {
                        "node_id": "vector_unpacking",
                        "type": "TRANSFORM",
                        "parents": ["source"],
                        "parameters": {
                            "transform_type": "custom_code",
                            "custom_code": """
                            import json
                            import numpy as np
                            
                            def process(df):
                                # Unpack vector columns
                                vector_columns = [
                                    'genre_vector', 'skill_vector', 'industry_vector',
                                    'title_embedding', 'description_embedding',
                                    'category_vector', 'topic_vector'
                                ]
                                
                                for col in vector_columns:
                                    if col in df.columns:
                                        df[col] = df[col].apply(json.loads)
                                        # Convert to numpy arrays
                                        df[col] = df[col].apply(np.array)
                                
                                return df
                            """
                        }
                    },
                    {
                        "node_id": "feature_scaling",
                        "type": "TRANSFORM",
                        "parents": ["vector_unpacking"],
                        "parameters": {
                            "transform_type": "standard_scaler",
                            "columns": [
                                "artist_diversity", "music_recency",
                                "social_engagement", "collaboration_score",
                                "engagement_level", "exploration_score"
                            ]
                        }
                    },
                    {
                        "node_id": "train_validation_split",
                        "type": "TRANSFORM",
                        "parents": ["feature_scaling"],
                        "parameters": {
                            "transform_type": "split",
                            "train_ratio": 0.8,
                            "validation_ratio": 0.2,
                            "random_seed": 42
                        }
                    }
                ]
            }
            
            # Create flow
            self.sagemaker_session.sagemaker_client.create_data_wrangler_flow(
                FlowSource=self.flow_source,
                FlowName=flow_name,
                FlowDefinition=flow_config
            )
            
        except Exception as e:
            logger.error(f"Error creating feature transformation flow: {str(e)}")
            raise 