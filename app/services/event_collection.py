from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
from app.schemas.event import Event, PriceInfo, Location, SourceInfo, EventAttributes, EventMetadata

logger = logging.getLogger(__name__)

class EventbriteClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    async def search_events(self, **kwargs) -> List[Dict[str, Any]]:
        # TODO: Implement actual Eventbrite API call
        return []

def transform_eventbrite_event(event_data: Dict[str, Any]) -> Event:
    # TODO: Implement actual transformation logic
    return Event(
        event_id="",
        title="",
        description="",
        start_datetime=datetime.now(),
        location=Location(
            venue_name="",
            address="",
            city="",
            state="",
            country="",
            coordinates={"lat": 0.0, "lng": 0.0}
        ),
        categories=[],  # Empty list for now
        attributes=EventAttributes(
            indoor_outdoor="indoor",  # Default value
            age_restriction="all",    # Default value
            accessibility_features=[]
        ),
        source=SourceInfo(
            platform="eventbrite",
            url="",
            last_updated=datetime.now()
        ),
        metadata=EventMetadata(
            popularity_score=0.5,  # Default value
            tags=[]
        )
    )

def calculate_price_tier(price: float) -> str:
    if price == 0:
        return "free"
    elif price < 20:
        return "low"
    elif price < 50:
        return "medium"
    else:
        return "high"

def calculate_popularity_score(event_data: Dict[str, Any]) -> float:
    # TODO: Implement popularity calculation
    return 0.0

def extract_indoor_outdoor(description: str) -> str:
    keywords_indoor = ["indoor", "inside", "indoors"]
    keywords_outdoor = ["outdoor", "outside", "outdoors", "open-air"]
    
    description = description.lower()
    
    if any(keyword in description for keyword in keywords_indoor):
        return "indoor"
    elif any(keyword in description for keyword in keywords_outdoor):
        return "outdoor"
    return "unknown"

def extract_age_restriction(description: str) -> Optional[int]:
    # Look for common age restriction patterns (e.g., "18+", "21 and over")
    import re
    
    patterns = [
        r'(\d+)\s*\+',
        r'(\d+)\s*and\s*over',
        r'(\d+)\s*or\s*older'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, description)
        if match:
            return int(match.group(1))
    return None

def extract_accessibility_features(description: str) -> List[str]:
    features = []
    keywords = {
        "wheelchair accessible": "wheelchair_accessible",
        "sign language": "sign_language",
        "hearing impaired": "hearing_assistance",
        "ada compliant": "ada_compliant"
    }
    
    description = description.lower()
    for keyword, feature in keywords.items():
        if keyword in description:
            features.append(feature)
    
    return features

def extract_dress_code(description: str) -> Optional[str]:
    dress_codes = {
        "casual": ["casual", "come as you are"],
        "business casual": ["business casual", "smart casual"],
        "formal": ["formal", "black tie", "evening wear"],
        "costume": ["costume", "fancy dress", "dress up"]
    }
    
    description = description.lower()
    for code, keywords in dress_codes.items():
        if any(keyword in description for keyword in keywords):
            return code
    return None 