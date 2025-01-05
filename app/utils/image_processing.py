import os
import io
import httpx
from typing import Optional, Dict, List, Any, Union, cast, Tuple
import torch
from torchvision import transforms
from transformers import AutoImageProcessor, AutoModelForImageClassification
from PIL import Image
from google.cloud import vision_v1
from google.cloud import storage
import logging
from torch import Tensor
import numpy as np
from scipy.spatial import distance

logger = logging.getLogger(__name__)

class ImageProcessor:
    """Class for handling image processing and analysis tasks."""
    
    def __init__(self, storage_bucket: str):
        """Initialize the ImageProcessor.
        
        Args:
            storage_bucket: Name of the GCS bucket for storing images
        """
        self.storage_bucket = storage_bucket
        self.storage_client = storage.Client()
        self.vision_client = vision_v1.ImageAnnotatorClient()
        
        # Initialize scene classification model
        self.scene_processor = AutoImageProcessor.from_pretrained("microsoft/resnet-50")
        self.scene_model = AutoModelForImageClassification.from_pretrained("microsoft/resnet-50")
        self.transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])

    def _calculate_spatial_density(self, boxes: List[Tuple[float, float, float, float]], image_size: Tuple[int, int]) -> float:
        """Calculate spatial density based on bounding box distribution.
        
        Args:
            boxes: List of normalized bounding boxes (x1, y1, x2, y2)
            image_size: Tuple of (width, height)
            
        Returns:
            Spatial density score between 0 and 1
        """
        if not boxes:
            return 0.0
            
        # Convert normalized coordinates to pixels
        width, height = image_size
        pixel_boxes = [(x1 * width, y1 * height, x2 * width, y2 * height) 
                      for x1, y1, x2, y2 in boxes]
                      
        # Calculate box centers
        centers = [((x1 + x2) / 2, (y1 + y2) / 2) for x1, y1, x2, y2 in pixel_boxes]
        
        if len(centers) < 2:
            return 0.5  # Single person
            
        # Calculate average distance between centers
        distances = distance.pdist(centers)
        avg_distance = np.mean(distances)
        
        # Normalize by image diagonal
        diagonal = np.sqrt(width ** 2 + height ** 2)
        normalized_density = 1 - (avg_distance / diagonal)
        
        return normalized_density

    def _estimate_crowd_size(self, person_objects: List[Dict[str, Any]], image_size: Tuple[int, int]) -> Dict[str, Any]:
        """Estimate crowd size and density using object detection and spatial analysis.
        
        Args:
            person_objects: List of detected person objects with bounding boxes
            image_size: Tuple of (width, height)
            
        Returns:
            Dictionary with crowd density information
        """
        if not person_objects:
            return {
                "density": "low",
                "count_estimate": "<10",
                "confidence": 1.0,
                "spatial_score": 0.0
            }
            
        # Extract bounding boxes and confidence scores
        boxes = []
        scores = []
        for obj in person_objects:
            if obj.get("boundingPoly"):
                vertices = obj["boundingPoly"].vertices
                x_coords = [v.x for v in vertices]
                y_coords = [v.y for v in vertices]
                x1, y1 = min(x_coords) / image_size[0], min(y_coords) / image_size[1]
                x2, y2 = max(x_coords) / image_size[0], max(y_coords) / image_size[1]
                boxes.append((x1, y1, x2, y2))
                scores.append(obj.get("score", 0.0))
                
        # Calculate spatial density
        spatial_score = self._calculate_spatial_density(boxes, image_size)
        
        # Combine person count and spatial density for final estimate
        person_count = len(boxes)
        avg_confidence = np.mean(scores) if scores else 0.0
        
        if person_count > 30 and spatial_score > 0.7:
            density = "very_high"
            count_estimate = "100+"
        elif person_count > 20 or (person_count > 10 and spatial_score > 0.6):
            density = "high"
            count_estimate = "50-100"
        elif person_count > 10 or (person_count > 5 and spatial_score > 0.5):
            density = "medium"
            count_estimate = "10-50"
        else:
            density = "low"
            count_estimate = "<10"
            
        return {
            "density": density,
            "count_estimate": count_estimate,
            "confidence": float(avg_confidence),
            "spatial_score": float(spatial_score),
            "person_count": person_count
        }

    async def download_image(self, image_url: str) -> Optional[bytes]:
        """Download an image from a URL.
        
        Args:
            image_url: URL of the image to download
            
        Returns:
            Image bytes if successful, None otherwise
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(image_url)
                if response.status_code == 200:
                    return response.content
                logger.warning(f"Failed to download image from {image_url}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error downloading image from {image_url}: {str(e)}") # type: ignore
            return None
    
    async def store_image(self, image_bytes: bytes, event_id: str, image_index: int) -> str:
        """Store an image in Google Cloud Storage.
        
        Args:
            image_bytes: Raw image data
            event_id: ID of the associated event
            image_index: Index of the image for the event
            
        Returns:
            GCS URI of the stored image
        """
        bucket = self.storage_client.bucket(self.storage_bucket)
        blob_name = f"images/{event_id}/{image_index}.jpg"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(image_bytes, content_type="image/jpeg")
        return f"gs://{self.storage_bucket}/{blob_name}"
    
    async def analyze_image(self, image_bytes: bytes) -> Dict[str, Any]:
        """Analyze an image using various techniques.
        
        Args:
            image_bytes: Raw image data
            
        Returns:
            Dictionary containing analysis results
        """
        results = {}
        
        # Scene classification
        try:
            image = Image.open(io.BytesIO(image_bytes))
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Convert PIL Image to tensor using transform and cast to Tensor type
            tensor_image = cast(Tensor, self.transform(image))
            # Add batch dimension
            tensor_image = tensor_image.unsqueeze(0)
            
            with torch.no_grad():
                outputs = self.scene_model(tensor_image)
                probs = torch.nn.functional.softmax(outputs.logits, dim=1)
                
            # Get top 5 predictions
            top5_prob, top5_indices = torch.topk(probs, 5)
            scene_results = {
                self.scene_model.config.id2label[idx.item()]: prob.item()
                for prob, idx in zip(top5_prob[0], top5_indices[0])
            }
            results["scene_classification"] = scene_results
        except Exception as e:
            logger.error(f"Error in scene classification: {str(e)}")
            
        # Google Cloud Vision analysis
        try:
            vision_image = vision_v1.Image(content=image_bytes)
            
            # Object detection with bounding boxes
            features = [
                {"type_": vision_v1.Feature.Type.OBJECT_LOCALIZATION},
                {"type_": vision_v1.Feature.Type.LABEL_DETECTION}
            ]
            request = {"image": vision_image, "features": features}
            response = self.vision_client.annotate_image(request)
            
            # Extract person objects with bounding boxes
            person_objects = [
                {
                    "name": obj.name,
                    "score": obj.score,
                    "boundingPoly": obj.bounding_poly
                }
                for obj in response.localized_object_annotations
                if obj.name.lower() in ["person", "man", "woman", "child"]
            ]
            
            # Get image size
            img = Image.open(io.BytesIO(image_bytes))
            image_size = img.size
            
            # Estimate crowd density
            results["crowd_density"] = self._estimate_crowd_size(person_objects, image_size)
            
            # General object detection
            results["objects"] = [
                {"name": label.description, "confidence": label.score}
                for label in response.label_annotations
            ]
            
            # Safe search detection
            features = [{"type_": vision_v1.Feature.Type.SAFE_SEARCH_DETECTION}]
            request = {"image": vision_image, "features": features}
            safe_search = self.vision_client.annotate_image(request)
            annotation = safe_search.safe_search_annotation
            results["safe_search"] = {
                "adult": annotation.adult.name,
                "violence": annotation.violence.name,
                "racy": annotation.racy.name
            }
            
        except Exception as e:
            logger.error(f"Error in Google Cloud Vision analysis: {str(e)}")
        
        return results 