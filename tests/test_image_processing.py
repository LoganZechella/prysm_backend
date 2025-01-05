import pytest
from unittest.mock import patch, MagicMock
from app.services.image_processing import (
    ImageProcessor,
    process_image,
    analyze_image,
    store_image
)
from app.schemas.event import Event, ImageAnalysis

def test_process_image():
    """Test image processing."""
    with patch("app.services.image_processing.ImageProcessor") as mock_processor:
        mock_instance = mock_processor.return_value
        mock_instance.process_image.return_value = {
            "url": "test.jpg",
            "width": 800,
            "height": 600,
            "format": "JPEG"
        }
        
        result = process_image("test.jpg")
        assert result["url"] == "test.jpg"
        assert result["width"] == 800
        assert result["height"] == 600
        assert result["format"] == "JPEG"

def test_analyze_image():
    """Test image analysis."""
    with patch("app.services.image_processing.ImageProcessor") as mock_processor:
        mock_instance = mock_processor.return_value
        mock_instance.analyze_image.return_value = {
            "labels": ["music", "concert", "crowd"],
            "objects": ["stage", "people", "instruments"],
            "text": ["LIVE", "MUSIC"],
            "safe_search": {
                "adult": "UNLIKELY",
                "violence": "UNLIKELY"
            }
        }
        
        result = analyze_image("test.jpg")
        assert "labels" in result
        assert "objects" in result
        assert "text" in result
        assert "safe_search" in result
        assert "music" in result["labels"]
        assert "stage" in result["objects"]

def test_store_image():
    """Test image storage."""
    with patch("app.services.image_processing.ImageProcessor") as mock_processor:
        mock_instance = mock_processor.return_value
        mock_instance.store_image.return_value = "gs://bucket/test.jpg"
        
        result = store_image("test.jpg", "test-event", 0)
        assert result == "gs://bucket/test.jpg" 