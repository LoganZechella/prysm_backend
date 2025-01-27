"""Training script for recommendation model."""
import os
import json
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
import pandas as pd
import logging
import argparse
from typing import Dict, Any, Tuple

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class RecommendationDataset(Dataset):
    """Dataset for recommendation model training."""
    
    def __init__(self, data_path: str):
        """Initialize dataset.
        
        Args:
            data_path: Path to data directory
        """
        # Load data
        self.data = pd.read_parquet(data_path)
        
        # Convert vector columns from strings back to arrays
        vector_columns = [
            'genre_vector', 'skill_vector', 'industry_vector',
            'title_embedding', 'description_embedding',
            'category_vector', 'topic_vector'
        ]
        
        for col in vector_columns:
            if col in self.data.columns:
                self.data[col] = self.data[col].apply(json.loads)
                self.data[col] = self.data[col].apply(np.array)
    
    def __len__(self) -> int:
        return len(self.data)
    
    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor]:
        """Get item from dataset.
        
        Args:
            idx: Index of item
            
        Returns:
            Tuple of (features, target)
        """
        row = self.data.iloc[idx]
        
        # Combine all features into a single vector
        features = np.concatenate([
            row['genre_vector'],
            row['skill_vector'],
            row['industry_vector'],
            row['title_embedding'],
            row['description_embedding'],
            row['category_vector'],
            row['topic_vector'],
            [
                row['artist_diversity'],
                row['music_recency'],
                row['music_consistency'],
                row['social_engagement'],
                row['collaboration_score'],
                row['discovery_score'],
                row['engagement_level'],
                row['exploration_score'],
                row['price_normalized'],
                row['capacity_normalized'],
                row['time_of_day'] / 24.0,
                row['day_of_week'] / 7.0,
                row['duration_hours'] / 24.0,
                row['attendance_score'],
                row['social_score'],
                row['organizer_rating']
            ]
        ])
        
        # Target is the match score (to be implemented)
        target = np.array([0.5])  # Placeholder
        
        return torch.FloatTensor(features), torch.FloatTensor(target)

class RecommendationModel(nn.Module):
    """Neural network for recommendation scoring."""
    
    def __init__(self, input_dim: int, embedding_dim: int):
        """Initialize model.
        
        Args:
            input_dim: Dimension of input features
            embedding_dim: Dimension of embeddings
        """
        super().__init__()
        
        self.network = nn.Sequential(
            nn.Linear(input_dim, embedding_dim),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(embedding_dim, embedding_dim // 2),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(embedding_dim // 2, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass.
        
        Args:
            x: Input tensor
            
        Returns:
            Output tensor
        """
        return self.network(x)

def train(args: argparse.Namespace) -> None:
    """Train the model.
    
    Args:
        args: Command line arguments
    """
    # Set device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # Load datasets
    train_dataset = RecommendationDataset(os.path.join(args.train, "training.parquet"))
    val_dataset = RecommendationDataset(os.path.join(args.validation, "validation.parquet"))
    
    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=True,
        num_workers=4
    )
    
    val_loader = DataLoader(
        val_dataset,
        batch_size=args.batch_size,
        shuffle=False,
        num_workers=4
    )
    
    # Initialize model
    sample_features, _ = train_dataset[0]
    input_dim = sample_features.shape[0]
    
    model = RecommendationModel(
        input_dim=input_dim,
        embedding_dim=args.embedding_dim
    ).to(device)
    
    # Define loss and optimizer
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=args.learning_rate)
    
    # Training loop
    best_val_loss = float('inf')
    
    for epoch in range(args.epochs):
        # Training
        model.train()
        train_loss = 0.0
        
        for features, targets in train_loader:
            features = features.to(device)
            targets = targets.to(device)
            
            optimizer.zero_grad()
            outputs = model(features)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item()
        
        train_loss /= len(train_loader)
        
        # Validation
        model.eval()
        val_loss = 0.0
        
        with torch.no_grad():
            for features, targets in val_loader:
                features = features.to(device)
                targets = targets.to(device)
                
                outputs = model(features)
                loss = criterion(outputs, targets)
                val_loss += loss.item()
        
        val_loss /= len(val_loader)
        
        # Log metrics
        logger.info(
            f"Epoch {epoch+1}/{args.epochs} - "
            f"Train Loss: {train_loss:.4f} - "
            f"Val Loss: {val_loss:.4f}"
        )
        
        # Save best model
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            model_path = os.path.join(args.model_dir, "model.pth")
            torch.save(model.state_dict(), model_path)
    
    # Save model config
    config = {
        "input_dim": input_dim,
        "embedding_dim": args.embedding_dim,
        "best_val_loss": best_val_loss
    }
    
    with open(os.path.join(args.model_dir, "model_config.json"), "w") as f:
        json.dump(config, f)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    # Data arguments
    parser.add_argument("--train", type=str, default=os.environ.get("SM_CHANNEL_TRAINING"))
    parser.add_argument("--validation", type=str, default=os.environ.get("SM_CHANNEL_VALIDATION"))
    parser.add_argument("--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    
    # Training arguments
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--learning-rate", type=float, default=0.001)
    parser.add_argument("--embedding-dim", type=int, default=128)
    
    args = parser.parse_args()
    
    train(args) 