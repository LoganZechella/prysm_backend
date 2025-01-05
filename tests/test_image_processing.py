import pytest
from datetime import datetime
import os
from app.utils.image_processing import ImageProcessor
from app.utils.schema import Event, ImageAnalysis, Location, SourceInfo
import httpx
from unittest.mock import Mock, patch
import json
import torch
from google.cloud import vision_v1

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
        # Create mock response for object detection and label detection
        object_response = Mock()
        
        # Mock object localization
        person_object = Mock()
        person_object.name = "person"
        person_object.score = 0.95
        person_object.bounding_poly = Mock()
        person_object.bounding_poly.vertices = [
            Mock(x=100, y=100),
            Mock(x=200, y=100),
            Mock(x=200, y=300),
            Mock(x=100, y=300)
        ]
        
        crowd_object = Mock()
        crowd_object.name = "person"
        crowd_object.score = 0.92
        crowd_object.bounding_poly = Mock()
        crowd_object.bounding_poly.vertices = [
            Mock(x=300, y=150),
            Mock(x=400, y=150),
            Mock(x=400, y=350),
            Mock(x=300, y=350)
        ]
        
        object_response.localized_object_annotations = [person_object, crowd_object]
        
        # Mock label detection
        label1 = Mock(description="concert", score=0.95)
        label2 = Mock(description="crowd", score=0.90)
        label3 = Mock(description="stage", score=0.85)
        object_response.label_annotations = [label1, label2, label3]
        
        # Create mock response for safe search
        safe_search_response = Mock()
        safe_search_annotation = Mock()
        safe_search_annotation.adult = Mock(name="VERY_UNLIKELY")
        safe_search_annotation.violence = Mock(name="UNLIKELY")
        safe_search_annotation.racy = Mock(name="POSSIBLE")
        safe_search_response.safe_search_annotation = safe_search_annotation
        
        # Configure mock to return different responses based on features
        def mock_annotate_image(request):
            features = request.get("features", [])
            if any(f["type_"] == vision_v1.Feature.Type.SAFE_SEARCH_DETECTION for f in features):
                return safe_search_response
            return object_response
            
        mock.return_value.annotate_image = mock_annotate_image
        yield mock

@pytest.fixture
def mock_storage_client():
    with patch("google.cloud.storage.Client") as mock:
        mock_blob = Mock()
        mock.return_value.bucket.return_value.blob.return_value = mock_blob
        mock_blob.upload_from_string = Mock()
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
def image_processor(mock_vision_client, mock_storage_client, mock_scene_model):
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
    stored_url = await image_processor.store_image(
        sample_image_bytes,
        "test-event-123",
        0
    )
    
    assert stored_url == "gs://test-bucket/images/test-event-123/0.jpg"

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
    assert "spatial_score" in analysis["crowd_density"]
    assert "person_count" in analysis["crowd_density"]
    assert analysis["crowd_density"]["person_count"] == 2
    
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
        async def mock_download(*args, **kwargs):
            return b"fake_image_data"
        
        async def mock_store(*args, **kwargs):
            return "gs://test-bucket/test-image.jpg"
        
        async def mock_analyze(*args, **kwargs):
            return {
                "scene_classification": {"concert_hall": 0.9},
                "crowd_density": {
                    "density": "high",
                    "count_estimate": "50+",
                    "confidence": 0.8,
                    "spatial_score": 0.75,
                    "person_count": 35
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
        
        mock_processor.download_image = mock_download
        mock_processor.store_image = mock_store
        mock_processor.analyze_image = mock_analyze
        
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