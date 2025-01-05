from typing import Dict, Any, Optional
from google.cloud import vision_v1, storage
import httpx
from PIL import Image
import io
import asyncio

class ImageProcessor:
    """Handles image processing and analysis."""
    
    def __init__(self, bucket_name: str):
        """Initialize with GCS bucket name."""
        self.bucket_name = bucket_name
        self.vision_client = vision_v1.ImageAnnotatorClient()
        self.storage_client = storage.Client()
        
    async def download_image(self, url: str) -> Optional[bytes]:
        """Download image from URL."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.content
                return None
        except Exception:
            return None
            
    async def store_image(self, image_data: bytes, event_id: str, index: int) -> str:
        """Store image in GCS."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob_name = f"images/{event_id}/{index}.jpg"
        blob = bucket.blob(blob_name)
        
        # Convert to JPEG if needed
        try:
            image = Image.open(io.BytesIO(image_data))
            if image.format != "JPEG":
                output = io.BytesIO()
                image.convert("RGB").save(output, format="JPEG")
                image_data = output.getvalue()
        except Exception:
            pass
            
        blob.upload_from_string(image_data, content_type="image/jpeg")
        return f"gs://{self.bucket_name}/{blob_name}"
        
    async def analyze_image(self, image_data: bytes) -> Dict[str, Any]:
        """Analyze image using Vision API."""
        image = vision_v1.Image(content=image_data)
        
        # Configure features
        features = [
            {"type_": vision_v1.Feature.Type.LABEL_DETECTION},
            {"type_": vision_v1.Feature.Type.OBJECT_LOCALIZATION},
            {"type_": vision_v1.Feature.Type.TEXT_DETECTION},
            {"type_": vision_v1.Feature.Type.SAFE_SEARCH_DETECTION}
        ]
        
        # Get annotations
        response = self.vision_client.annotate_image({
            "image": image,
            "features": features
        })
        
        # Extract labels
        labels = [label.description for label in response.label_annotations]
        
        # Extract objects
        objects = [obj.name for obj in response.localized_object_annotations]
        
        # Extract text
        text = []
        if response.text_annotations:
            text = response.text_annotations[0].description.split()
            
        # Extract safe search
        safe_search = {
            "adult": response.safe_search_annotation.adult.name,
            "violence": response.safe_search_annotation.violence.name
        }
        
        return {
            "labels": labels,
            "objects": objects,
            "text": text,
            "safe_search": safe_search
        }

def process_image(image_path: str) -> Dict[str, Any]:
    """Process an image and return basic information."""
    try:
        with Image.open(image_path) as img:
            return {
                "url": image_path,
                "width": img.width,
                "height": img.height,
                "format": img.format
            }
    except Exception:
        return {}

def analyze_image(image_path: str) -> Dict[str, Any]:
    """Analyze an image using Vision API."""
    try:
        with open(image_path, "rb") as image_file:
            processor = ImageProcessor("temp-bucket")
            return asyncio.run(processor.analyze_image(image_file.read()))
    except Exception:
        return {}

def store_image(image_path: str, event_id: str, index: int) -> str:
    """Store an image in GCS."""
    try:
        with open(image_path, "rb") as image_file:
            processor = ImageProcessor("temp-bucket")
            return asyncio.run(processor.store_image(image_file.read(), event_id, index))
    except Exception:
        return "" 