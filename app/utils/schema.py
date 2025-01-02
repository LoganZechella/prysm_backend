from typing import Dict, Any, Optional
from datetime import datetime
import json
from pydantic import BaseModel, Field, validator
from typing import List, Union

class Location(BaseModel):
    venue_name: str
    address: str
    city: str
    state: str
    country: str
    coordinates: Dict[str, float] = Field(
        default_factory=lambda: {"lat": 0.0, "lng": 0.0}
    )

class PriceInfo(BaseModel):
    currency: str = "USD"
    min_price: float = 0.0
    max_price: Optional[float] = None
    price_tier: str = "free"  # free, budget, medium, premium

    @validator('price_tier')
    def validate_price_tier(cls, v):
        allowed = ['free', 'budget', 'medium', 'premium']
        if v not in allowed:
            raise ValueError(f'price_tier must be one of {allowed}')
        return v

class EventAttributes(BaseModel):
    indoor_outdoor: str = "indoor"  # indoor, outdoor, both
    age_restriction: str = "all"  # all, 18+, 21+
    accessibility_features: List[str] = Field(default_factory=list)
    dress_code: Optional[str] = None  # casual, formal, business

    @validator('indoor_outdoor')
    def validate_indoor_outdoor(cls, v):
        allowed = ['indoor', 'outdoor', 'both']
        if v not in allowed:
            raise ValueError(f'indoor_outdoor must be one of {allowed}')
        return v

    @validator('age_restriction')
    def validate_age_restriction(cls, v):
        allowed = ['all', '18+', '21+']
        if v not in allowed:
            raise ValueError(f'age_restriction must be one of {allowed}')
        return v

class SourceInfo(BaseModel):
    platform: str
    url: str
    last_updated: datetime = Field(default_factory=datetime.utcnow)

class EventMetadata(BaseModel):
    popularity_score: float = 0.0
    social_mentions: int = 0
    verified: bool = False

class Event(BaseModel):
    event_id: str
    title: str
    description: str
    short_description: Optional[str] = None
    start_datetime: datetime
    end_datetime: Optional[datetime] = None
    location: Location
    categories: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    price_info: PriceInfo
    attributes: EventAttributes = Field(default_factory=EventAttributes)
    extracted_topics: List[str] = Field(default_factory=list)
    sentiment_scores: Dict[str, float] = Field(
        default_factory=lambda: {"positive": 0.0, "negative": 0.0}
    )
    images: List[str] = Field(default_factory=list)
    source: SourceInfo
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    @validator('end_datetime', always=True)
    def set_end_datetime(cls, v, values):
        if v is None and 'start_datetime' in values:
            # Default to 3 hours after start if not specified
            return values['start_datetime'].replace(hour=values['start_datetime'].hour + 3)
        return v

    def to_json(self) -> str:
        """Convert event to JSON string"""
        return self.json(exclude_unset=True)

    @classmethod
    def from_json(cls, json_str: str) -> 'Event':
        """Create event from JSON string"""
        return cls.parse_raw(json_str)

    @classmethod
    def from_raw_data(cls, raw_data: Dict[str, Any]) -> 'Event':
        """Create event from raw data dictionary"""
        # Convert string dates to datetime objects if needed
        if isinstance(raw_data.get('start_datetime'), str):
            raw_data['start_datetime'] = datetime.fromisoformat(raw_data['start_datetime'])
        if isinstance(raw_data.get('end_datetime'), str):
            raw_data['end_datetime'] = datetime.fromisoformat(raw_data['end_datetime'])
        
        return cls(**raw_data) 