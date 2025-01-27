"""SageMaker Pipeline definition for recommendation model training."""
import os
from typing import Dict, Any

from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import (
    ProcessingStep, 
    TrainingStep,
    CreateModelStep
)
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.inputs import TrainingInput
from sagemaker.pytorch import PyTorch
from sagemaker.model import Model
from sagemaker import get_execution_role

class RecommendationPipeline:
    """SageMaker Pipeline for training recommendation models."""
    
    def __init__(
        self,
        role: str = None,
        pipeline_name: str = "recommendation-training",
        base_job_prefix: str = "recommendation",
    ):
        """Initialize pipeline.
        
        Args:
            role: SageMaker execution role
            pipeline_name: Name of the pipeline
            base_job_prefix: Prefix for all jobs in the pipeline
        """
        self.role = role or get_execution_role()
        self.pipeline_name = pipeline_name
        self.base_job_prefix = base_job_prefix
        
    def _get_preprocessing_step(
        self,
        feature_store_uri: str,
        instance_type: str = "ml.m5.xlarge"
    ) -> ProcessingStep:
        """Create preprocessing step."""
        processor = PyTorch(
            framework_version="2.0.1",
            py_version="py39",
            role=self.role,
            instance_count=1,
            instance_type=instance_type,
            base_job_name=f"{self.base_job_prefix}/preprocessing"
        )
        
        return ProcessingStep(
            name="PreprocessingStep",
            processor=processor,
            inputs=[
                ProcessingInput(
                    source=feature_store_uri,
                    destination="/opt/ml/processing/input"
                )
            ],
            outputs=[
                ProcessingOutput(
                    output_name="training",
                    source="/opt/ml/processing/train"
                ),
                ProcessingOutput(
                    output_name="validation",
                    source="/opt/ml/processing/validation"
                ),
                ProcessingOutput(
                    output_name="test",
                    source="/opt/ml/processing/test"
                )
            ],
            code="scripts/preprocess.py"
        )
        
    def _get_training_step(
        self,
        preprocessing_step: ProcessingStep,
        hyperparameters: Dict[str, Any],
        instance_type: str = "ml.m5.xlarge"
    ) -> TrainingStep:
        """Create training step."""
        estimator = PyTorch(
            entry_point="train.py",
            source_dir="scripts",
            framework_version="2.0.1",
            py_version="py39",
            role=self.role,
            instance_count=1,
            instance_type=instance_type,
            hyperparameters=hyperparameters,
            base_job_name=f"{self.base_job_prefix}/training"
        )
        
        return TrainingStep(
            name="TrainingStep",
            estimator=estimator,
            inputs={
                "training": TrainingInput(
                    s3_data=preprocessing_step.properties.ProcessingOutputConfig.Outputs[
                        "training"
                    ].S3Output.S3Uri
                ),
                "validation": TrainingInput(
                    s3_data=preprocessing_step.properties.ProcessingOutputConfig.Outputs[
                        "validation"
                    ].S3Output.S3Uri
                )
            }
        )
        
    def _get_evaluation_step(
        self,
        training_step: TrainingStep,
        preprocessing_step: ProcessingStep,
        instance_type: str = "ml.m5.xlarge"
    ) -> ProcessingStep:
        """Create evaluation step."""
        processor = PyTorch(
            framework_version="2.0.1",
            py_version="py39",
            role=self.role,
            instance_count=1,
            instance_type=instance_type,
            base_job_name=f"{self.base_job_prefix}/evaluation"
        )
        
        return ProcessingStep(
            name="EvaluationStep",
            processor=processor,
            inputs=[
                ProcessingInput(
                    source=training_step.properties.ModelArtifacts.S3ModelArtifacts,
                    destination="/opt/ml/processing/model"
                ),
                ProcessingInput(
                    source=preprocessing_step.properties.ProcessingOutputConfig.Outputs[
                        "test"
                    ].S3Output.S3Uri,
                    destination="/opt/ml/processing/test"
                )
            ],
            outputs=[
                ProcessingOutput(
                    output_name="evaluation",
                    source="/opt/ml/processing/evaluation"
                )
            ],
            code="scripts/evaluate.py"
        )
        
    def _get_create_model_step(
        self,
        training_step: TrainingStep,
        instance_type: str = "ml.m5.xlarge"
    ) -> CreateModelStep:
        """Create model creation step."""
        model = Model(
            image_uri=training_step.properties.AlgorithmSpecification.TrainingImage,
            model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
            role=self.role,
            sagemaker_session=None
        )
        
        return CreateModelStep(
            name="CreateModelStep",
            model=model,
            inputs=training_step.properties.ModelArtifacts.S3ModelArtifacts
        )
        
    def build(
        self,
        feature_store_uri: str,
        hyperparameters: Dict[str, Any]
    ) -> Pipeline:
        """Build the pipeline.
        
        Args:
            feature_store_uri: URI of feature store data
            hyperparameters: Model hyperparameters
            
        Returns:
            SageMaker Pipeline
        """
        # Create steps
        preprocessing_step = self._get_preprocessing_step(feature_store_uri)
        training_step = self._get_training_step(preprocessing_step, hyperparameters)
        evaluation_step = self._get_evaluation_step(training_step, preprocessing_step)
        create_model_step = self._get_create_model_step(training_step)
        
        # Create pipeline
        pipeline = Pipeline(
            name=self.pipeline_name,
            parameters=[],
            steps=[
                preprocessing_step,
                training_step, 
                evaluation_step,
                create_model_step
            ]
        )
        
        return pipeline 