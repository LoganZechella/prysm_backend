import os
import io
import httpx
from typing import Optional, Dict, List, Any, Union, cast
import torch
from torchvision import transforms
from transformers import AutoImageProcessor, AutoModelForImageClassification
from PIL import Image
from google.cloud import vision_v1
from google.cloud import storage
import logging
from torch import Tensor

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
            logger.error(f"Error downloading image from {image_url}: {str(e)}")
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
            
            # Label detection
            features = [{"type_": vision_v1.Feature.Type.LABEL_DETECTION}]
            request = {"image": vision_image, "features": features}
            response = self.vision_client.annotate_image(request)
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
            
            # Crowd density estimation (using object detection)
            person_labels = [label for label in response.label_annotations 
                           if label.description.lower() in ["person", "crowd", "audience"]]
            if person_labels:
                max_person_score = max(label.score for label in person_labels)
                density = "high" if max_person_score > 0.8 else "medium" if max_person_score > 0.5 else "low"
                results["crowd_density"] = {
                    "density": density,
                    "count_estimate": "50+" if density == "high" else "10-50" if density == "medium" else "<10",
                    "confidence": max_person_score
                }
            else:
                results["crowd_density"] = {
                    "density": "low",
                    "count_estimate": "<10",
                    "confidence": 1.0
                }
            
        except Exception as e:
            logger.error(f"Error in Google Cloud Vision analysis: {str(e)}")
        
        return results 