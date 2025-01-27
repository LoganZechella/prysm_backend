"""Evaluation script for recommendation model."""
import os
import json
import torch
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, precision_recall_curve, average_precision_score
import logging
from typing import Dict, Any, Tuple
from train import RecommendationModel, RecommendationDataset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_model(model_dir: str) -> Tuple[RecommendationModel, Dict[str, Any]]:
    """Load trained model and config.
    
    Args:
        model_dir: Directory containing model artifacts
        
    Returns:
        Tuple of (model, config)
    """
    # Load config
    with open(os.path.join(model_dir, "model_config.json"), "r") as f:
        config = json.load(f)
    
    # Initialize model
    model = RecommendationModel(
        input_dim=config["input_dim"],
        embedding_dim=config["embedding_dim"]
    )
    
    # Load state dict
    model.load_state_dict(torch.load(
        os.path.join(model_dir, "model.pth"),
        map_location=torch.device('cpu')
    ))
    
    return model, config

def evaluate(
    model: RecommendationModel,
    validation_data: str
) -> Dict[str, float]:
    """Evaluate model performance.
    
    Args:
        model: Trained model
        validation_data: Path to validation data
        
    Returns:
        Dictionary of evaluation metrics
    """
    # Load validation dataset
    dataset = RecommendationDataset(validation_data)
    
    # Get predictions
    model.eval()
    all_predictions = []
    all_targets = []
    
    with torch.no_grad():
        for features, targets in dataset:
            outputs = model(features)
            all_predictions.append(outputs.numpy())
            all_targets.append(targets.numpy())
    
    predictions = np.concatenate(all_predictions)
    targets = np.concatenate(all_targets)
    
    # Calculate metrics
    try:
        auc_roc = roc_auc_score(targets, predictions)
    except:
        auc_roc = 0.0
    
    precision, recall, _ = precision_recall_curve(targets, predictions)
    avg_precision = average_precision_score(targets, predictions)
    
    # Calculate recommendation quality metrics
    top_k_precision = np.mean([
        np.mean(targets[predictions.argsort()[-k:]])
        for k in [5, 10, 20]
    ])
    
    return {
        "auc_roc": float(auc_roc),
        "average_precision": float(avg_precision),
        "top_k_precision": float(top_k_precision)
    }

if __name__ == "__main__":
    # Load model
    model_dir = "/opt/ml/processing/model"
    validation_dir = "/opt/ml/processing/validation"
    output_dir = "/opt/ml/processing/evaluation"
    
    model, config = load_model(model_dir)
    
    # Evaluate model
    metrics = evaluate(
        model=model,
        validation_data=os.path.join(validation_dir, "validation.parquet")
    )
    
    # Log metrics
    for name, value in metrics.items():
        logger.info(f"{name}: {value:.4f}")
    
    # Save metrics
    os.makedirs(output_dir, exist_ok=True)
    
    evaluation_path = os.path.join(output_dir, "evaluation.json")
    with open(evaluation_path, "w") as f:
        json.dump(metrics, f) 