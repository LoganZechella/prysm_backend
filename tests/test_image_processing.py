import pytest
from datetime import datetime
import os
from app.utils.image_processing import ImageProcessor
from app.utils.schema import Event, ImageAnalysis, Location, SourceInfo
import httpx
from unittest.mock import Mock, patch
import json
import torch

@pytest.fixture
def sample_image_bytes():
    """Load a sample test image"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    image_path = os.path.join(current_dir, "data", "test_image.jpg")
    with open(image_path, "rb") as f:
        return f.read()

@pytest.fixture
def mock_vision_client():
    with patch("google.cloud.vision_v1.ImageAnnotatorClient") as mock:
        # Mock label detection
        label_response = Mock()
        label_response.label_annotations = [
            Mock(description="concert", score=0.95),
            Mock(description="crowd", score=0.90),
            Mock(description="stage", score=0.85)
        ]
        mock.return_value.label_detection.return_value = label_response
        
        # Mock safe search detection
        safe_search_response = Mock()
        safe_search_response.safe_search_annotation = Mock(
            adult="VERY_UNLIKELY",
            violence="UNLIKELY",
            racy="POSSIBLE"
        )
        mock.return_value.annotate_image.return_value = safe_search_response
        
        yield mock

@pytest.fixture
def mock_scene_model():
    with patch("transformers.AutoModelForImageClassification") as mock:
        outputs = Mock()
        outputs.logits = torch.tensor([[2.0, 1.0, 0.5]])  # Mock logits
        mock.return_value.return_value = outputs
        mock.return_value.config.id2label = {
            0: "concert_hall",
            1: "stage",
            2: "crowd"
        }
        yield mock

@pytest.fixture
def image_processor(mock_vision_client, mock_scene_model):
    return ImageProcessor("test-bucket")

@pytest.mark.asyncio
async def test_download_image():
    """Test image downloading"""
    processor = ImageProcessor("test-bucket")
    
    # Test successful download
    async def mock_successful_get(*args, **kwargs):
        return Mock(status_code=200, content=b"fake_image_data")
    
    with patch("httpx.AsyncClient.get", mock_successful_get):
        image_data = await processor.download_image("https://example.com/image.jpg")
        assert image_data == b"fake_image_data"
    
    # Test failed download
    async def mock_failed_get(*args, **kwargs):
        return Mock(status_code=404)
    
    with patch("httpx.AsyncClient.get", mock_failed_get):
        image_data = await processor.download_image("https://example.com/not_found.jpg")
        assert image_data is None

@pytest.mark.asyncio
async def test_store_image(image_processor, sample_image_bytes):
    """Test image storage"""
    # Mock storage client
    with patch("google.cloud.storage.Client") as mock_storage:
        mock_blob = Mock()
        mock_storage.return_value.bucket.return_value.blob.return_value = mock_blob
        
        stored_url = await image_processor.store_image(
            sample_image_bytes,
            "test-event-123",
            0
        )
        
        assert stored_url == "gs://test-bucket/images/test-event-123/0.jpg"
        assert mock_blob.upload_from_string.called

@pytest.mark.asyncio
async def test_analyze_image(image_processor, sample_image_bytes):
    """Test image analysis"""
    analysis = await image_processor.analyze_image(sample_image_bytes)
    
    # Check scene classification
    assert "scene_classification" in analysis
    assert len(analysis["scene_classification"]) > 0
    assert all(isinstance(v, float) for v in analysis["scene_classification"].values())
    
    # Check crowd density
    assert "crowd_density" in analysis
    assert "density" in analysis["crowd_density"]
    assert "count_estimate" in analysis["crowd_density"]
    assert "confidence" in analysis["crowd_density"]
    
    # Check object detection
    assert "objects" in analysis
    assert len(analysis["objects"]) > 0
    assert all("name" in obj and "confidence" in obj for obj in analysis["objects"])
    
    # Check safe search
    assert "safe_search" in analysis
    assert all(k in analysis["safe_search"] for k in ["adult", "violence", "racy"])

@pytest.mark.asyncio
async def test_event_image_processing():
    """Test end-to-end event image processing"""
    # Create test event
    event = Event(
        event_id="test-123",
        title="Test Concert",
        description="A great concert",
        start_datetime=datetime.utcnow(),
        location=Location(
            venue_name="Test Venue",
            address="123 Test St",
            city="Test City",
            state="TC",
            country="Test Country",
            coordinates={"lat": 0.0, "lng": 0.0}
        ),
        source=SourceInfo(
            platform="test",
            url="https://test.com",
            last_updated=datetime.utcnow()
        ),
        images=["https://example.com/image1.jpg", "https://example.com/image2.jpg"]
    )
    
    # Mock image processor
    with patch("app.utils.event_processing.image_processor") as mock_processor:
        # Mock successful image processing
        mock_processor.download_image.return_value = b"fake_image_data"
        mock_processor.store_image.return_value = "gs://test-bucket/test-image.jpg"
        mock_processor.analyze_image.return_value = {
            "scene_classification": {"concert_hall": 0.9},
            "crowd_density": {
                "density": "high",
                "count_estimate": "50+",
                "confidence": 0.8
            },
            "objects": [
                {"name": "stage", "confidence": 0.95},
                {"name": "crowd", "confidence": 0.9}
            ],
            "safe_search": {
                "adult": "UNLIKELY",
                "violence": "VERY_UNLIKELY",
                "racy": "POSSIBLE"
            }
        }
        
        # Process event
        from app.utils.event_processing import process_event
        processed_event = await process_event(event)
        
        # Verify image analysis results
        assert len(processed_event.image_analysis) == 2
        for analysis in processed_event.image_analysis:
            assert analysis.stored_url == "gs://test-bucket/test-image.jpg"
            assert "concert_hall" in analysis.scene_classification
            assert analysis.crowd_density["density"] == "high"
            assert len(analysis.objects) == 2
            assert all(k in analysis.safe_search for k in ["adult", "violence", "racy"]) 