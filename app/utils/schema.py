from typing import Dict, Any, Optional, Mapping, List, Set, Tuple
from datetime import datetime, timedelta, time
import json
from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator, ValidationInfo
from typing import List, Union

class Location(BaseModel):
    venue_name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    coordinates: Dict[str, float] = Field(
        default_factory=lambda: {"lat": 0.0, "lng": 0.0}
    )

class PriceInfo(BaseModel):
    currency: str = "USD"
    min_price: float = 0.0
    max_price: float = 0.0
    price_tier: str = "free"  # free, budget, medium, premium

    @field_validator('price_tier')
    @classmethod
    def validate_price_tier(cls, v: str) -> str:
        allowed = ['free', 'budget', 'medium', 'premium']
        if v not in allowed:
            raise ValueError(f'price_tier must be one of {allowed}')
        return v

class EventAttributes(BaseModel):
    indoor_outdoor: str = "indoor"  # indoor, outdoor, both
    age_restriction: str = "all"  # all, 18+, 21+
    accessibility_features: List[str] = Field(default_factory=list)
    dress_code: str = "casual"  # casual, business, formal

    @field_validator('indoor_outdoor')
    @classmethod
    def validate_indoor_outdoor(cls, v: str) -> str:
        allowed = ['indoor', 'outdoor', 'both']
        if v not in allowed:
            raise ValueError(f'indoor_outdoor must be one of {allowed}')
        return v

    @field_validator('age_restriction')
    @classmethod
    def validate_age_restriction(cls, v: str) -> str:
        allowed = ['all', '18+', '21+']
        if v not in allowed:
            raise ValueError(f'age_restriction must be one of {allowed}')
        return v

class SourceInfo(BaseModel):
    platform: str
    url: str
    last_updated: datetime

    model_config = ConfigDict(
        ser_json_timedelta="iso8601",
        ser_json_bytes="base64",
        json_schema_extra={"format": "date-time"}
    )

class EventMetadata(BaseModel):
    popularity_score: float = 0.0
    social_mentions: int = 0
    verified: bool = False

class CategoryNode(BaseModel):
    """Represents a node in the category hierarchy"""
    name: str
    parent: Optional[str] = None
    children: List[str] = Field(default_factory=list)
    keywords: List[str] = Field(default_factory=list)
    platform_mappings: Dict[str, List[str]] = Field(default_factory=dict)

class CategoryHierarchy(BaseModel):
    """Manages the hierarchical category structure"""
    nodes: Dict[str, CategoryNode] = Field(default_factory=dict)
    
    def add_category(self, name: str, parent: Optional[str] = None, 
                    keywords: Optional[List[str]] = None,
                    platform_mappings: Optional[Dict[str, List[str]]] = None) -> None:
        """Add a category to the hierarchy"""
        if name in self.nodes:
            return
            
        node = CategoryNode(
            name=name,
            parent=parent,
            keywords=keywords or [],
            platform_mappings=platform_mappings or {}
        )
        
        self.nodes[name] = node
        if parent and parent in self.nodes:
            self.nodes[parent].children.append(name)
    
    def get_ancestors(self, category: str) -> List[str]:
        """Get all ancestors of a category"""
        ancestors = []
        current = category
        
        while current in self.nodes and self.nodes[current].parent:
            parent = self.nodes[current].parent
            ancestors.append(parent)
            current = parent
            
        return ancestors
    
    def get_descendants(self, category: str) -> List[str]:
        """Get all descendants of a category"""
        if category not in self.nodes:
            return []
            
        descendants = []
        to_process = [category]
        
        while to_process:
            current = to_process.pop()
            children = self.nodes[current].children
            descendants.extend(children)
            to_process.extend(children)
            
        return descendants
    
    def map_platform_category(self, platform: str, platform_category: str) -> Optional[str]:
        """Map a platform-specific category to our hierarchy"""
        for category, node in self.nodes.items():
            if platform in node.platform_mappings:
                if platform_category in node.platform_mappings[platform]:
                    return category
        return None

class ImageAnalysis(BaseModel):
    """Results of image analysis"""
    url: str
    stored_url: Optional[str] = None
    scene_classification: Dict[str, float] = Field(default_factory=dict)
    crowd_density: Dict[str, Any] = Field(default_factory=dict)
    objects: List[Dict[str, Any]] = Field(default_factory=list)
    safe_search: Dict[str, str] = Field(default_factory=dict)

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
    price_info: PriceInfo = Field(default_factory=PriceInfo)
    attributes: EventAttributes = Field(default_factory=EventAttributes)
    extracted_topics: List[str] = Field(default_factory=list)
    sentiment_scores: Dict[str, float] = Field(default_factory=dict)
    images: List[str] = Field(default_factory=list)
    image_analysis: List[ImageAnalysis] = Field(default_factory=list)
    source: SourceInfo
    metadata: EventMetadata = Field(default_factory=EventMetadata)
    category_hierarchy: Dict[str, List[str]] = Field(default_factory=dict)
    raw_categories: List[str] = Field(default_factory=list)

    model_config = ConfigDict(
        ser_json_timedelta="iso8601",
        ser_json_bytes="base64",
        json_schema_extra={"format": "date-time"}
    )

    @field_validator("start_datetime", "end_datetime")
    def validate_datetime(cls, v: datetime) -> datetime:
        if not isinstance(v, datetime):
            raise ValueError("Invalid datetime format")
        return v

    def to_json(self) -> str:
        """Convert event to JSON string"""
        return self.model_dump_json(exclude_unset=True)

    @classmethod
    def from_json(cls, json_str: str) -> 'Event':
        """Create event from JSON string"""
        return cls.model_validate_json(json_str)

    @classmethod
    def from_raw_data(cls, raw_data: Dict[str, Any]) -> 'Event':
        """Create event from raw data dictionary"""
        # Convert string dates to datetime objects if needed
        if isinstance(raw_data.get('start_datetime'), str):
            raw_data['start_datetime'] = datetime.fromisoformat(raw_data['start_datetime'])
        if isinstance(raw_data.get('end_datetime'), str):
            raw_data['end_datetime'] = datetime.fromisoformat(raw_data['end_datetime'])
        
        return cls.model_validate(raw_data) 

    def add_category(self, category: str, hierarchy: CategoryHierarchy) -> None:
        """Add a category and its ancestors to the event"""
        if category not in self.categories:
            self.categories.append(category)
            self.category_hierarchy[category] = hierarchy.get_ancestors(category)
    
    def get_all_categories(self) -> Set[str]:
        """Get all categories including ancestors"""
        all_cats = set(self.categories)
        for category in self.categories:
            all_cats.update(self.category_hierarchy.get(category, []))
        return all_cats 

class UserPreferences(BaseModel):
    """User preferences for event recommendations"""
    user_id: str
    preferred_categories: List[str] = Field(default_factory=list)
    price_preferences: List[str] = Field(default_factory=list)  # List of acceptable price tiers
    preferred_location: Optional[Dict[str, float]] = None  # lat/lng coordinates
    preferred_radius: float = 50.0  # kilometers
    preferred_times: List[Tuple[time, time]] = Field(default_factory=list)  # List of time ranges
    excluded_categories: List[str] = Field(default_factory=list)
    min_rating: float = 0.0
    max_price: Optional[float] = None
    accessibility_requirements: List[str] = Field(default_factory=list)
    indoor_outdoor_preference: Optional[str] = None  # 'indoor', 'outdoor', or 'both'
    age_restriction_preference: Optional[str] = None  # '18+', '21+', 'all'
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_id": "user123",
                "preferred_categories": ["music", "art", "food"],
                "price_preferences": ["free", "budget", "medium"],
                "preferred_location": {"lat": 37.7749, "lng": -122.4194},
                "preferred_radius": 25.0,
                "preferred_times": [
                    ["18:00", "23:00"],
                    ["12:00", "15:00"]
                ],
                "excluded_categories": ["sports"],
                "min_rating": 4.0,
                "max_price": 100.0,
                "accessibility_requirements": ["wheelchair"],
                "indoor_outdoor_preference": "both",
                "age_restriction_preference": "all"
            }
        }
    )
    
    @classmethod
    def from_json(cls, json_str: str) -> 'UserPreferences':
        """Create preferences from JSON string"""
        return cls.model_validate_json(json_str)
    
    def to_json(self) -> str:
        """Convert preferences to JSON string"""
        return self.model_dump_json(exclude_unset=True) 