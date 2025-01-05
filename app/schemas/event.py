from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime

# Valid values for enums
PRICE_TIERS = ["free", "low", "medium", "high", "premium"]
INDOOR_OUTDOOR_OPTIONS = ["indoor", "outdoor", "both"]
AGE_RESTRICTIONS = ["all", "18+", "21+"]
EVENT_STATUSES = ["active", "cancelled", "postponed", "sold_out"]

class ImageAnalysis(BaseModel):
    """Analysis results for an event image"""
    url: str
    stored_url: Optional[str] = None
    scene_classification: Dict[str, float] = {}
    crowd_density: Dict[str, float] = {}
    objects: List[Dict[str, Any]] = []
    safe_search: Dict[str, str] = {}

class Location(BaseModel):
    venue_name: str
    coordinates: Dict[str, float]
    city: str
    address: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None

class PriceInfo(BaseModel):
    min_price: float = Field(ge=0)
    max_price: float = Field(ge=0)
    price_tier: str = Field(pattern=r"^(free|low|medium|high|premium)$")
    currency: str = "USD"

class EventAttributes(BaseModel):
    indoor_outdoor: str = Field(pattern=r"^(indoor|outdoor|both)$")
    age_restriction: str = Field(pattern=r"^(all|18\+|21\+)$")
    accessibility_features: List[str] = []
    dress_code: Optional[str] = None
    capacity: Optional[int] = None
    parking_available: Optional[bool] = None
    target_audience: List[str] = []  # e.g., ["family", "professional", "academic", "adult", "general"]

class SourceInfo(BaseModel):
    platform: str
    url: str
    last_updated: datetime
    external_id: Optional[str] = None

class EventMetadata(BaseModel):
    popularity_score: float = Field(ge=0, le=1)
    views: Optional[int] = None
    likes: Optional[int] = None
    shares: Optional[int] = None
    tags: List[str] = []
    sentiment_scores: Optional[Dict[str, float]] = None

class Event(BaseModel):
    event_id: str
    title: str
    description: str
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    location: Location
    categories: List[str]
    price_info: Optional[PriceInfo] = None
    attributes: EventAttributes
    source: SourceInfo
    metadata: EventMetadata
    image_url: Optional[str] = None
    ticket_url: Optional[str] = None
    organizer: Optional[str] = None
    status: str = Field(pattern=r"^(active|cancelled|postponed|sold_out)$", default="active")
    images: List[str] = []  # List of image URLs
    image_analysis: List[ImageAnalysis] = []  # Analysis results for each image
    raw_categories: List[str] = []  # Original categories from the source platform
    extracted_topics: List[str] = []  # Topics extracted from text analysis 