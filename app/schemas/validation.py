"""
Data validation schemas with versioning support.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing_extensions import Literal
from app.monitoring.performance import PerformanceMonitor

class SchemaVersion(str, Enum):
    V1 = "1.0"
    V2 = "2.0"

class Category(BaseModel):
    """Event category schema"""
    name: str
    parent: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)
    
    @field_validator('name')
    def validate_category_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Category name cannot be empty")
        if not re.match(r'^[a-zA-Z0-9\s\-_]+$', v):
            raise ValueError("Category name contains invalid characters")
        return v

class LocationBase(BaseModel):
    """Base location schema."""
    name: str
    address: Optional[str] = None
    city: str
    state: Optional[str] = None
    country: str
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    @field_validator('name')
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("Name cannot be empty")
        return v.strip()
        
    @field_validator('postal_code')
    def validate_postal_code(cls, v):
        if v and not v.strip():
            raise ValueError("Postal code cannot be empty if provided")
        return v.strip() if v else None

class PriceBase(BaseModel):
    """Base price schema."""
    currency: str
    amount: float
    tier: Optional[str] = None
    
    @field_validator('currency')
    def validate_currency(cls, v):
        if not v.strip():
            raise ValueError("Currency cannot be empty")
        return v.strip().upper()

class EventBase(BaseModel):
    """Base event schema."""
    title: str
    description: Optional[str] = None
    start_date: datetime
    end_date: Optional[datetime] = None
    location: LocationBase
    price_info: Optional[List[PriceBase]] = None
    categories: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    external_id: str
    source: str
    url: str
    
    model_config = ConfigDict(from_attributes=True)
    
    @field_validator('title')
    def validate_title(cls, v):
        if not v.strip():
            raise ValueError("Title cannot be empty")
        return v.strip()
        
    @field_validator('description')
    def validate_description(cls, v):
        if v and not v.strip():
            raise ValueError("Description cannot be empty if provided")
        return v.strip() if v else None
        
    @field_validator('end_date')
    def validate_end_date(cls, v, values):
        if v and 'start_date' in values.data and v < values.data['start_date']:
            raise ValueError("End date must be after start date")
        return v
        
    @field_validator('external_id')
    def validate_external_id(cls, v):
        if not v.strip():
            raise ValueError("External ID cannot be empty")
        return v.strip()

class EventV1(EventBase):
    """Schema version 1.0 for events."""
    schema_version: Literal[SchemaVersion.V1] = Field(default=SchemaVersion.V1)

class EventV2(EventBase):
    """Schema version 2.0 for events with additional fields."""
    schema_version: Literal[SchemaVersion.V2] = Field(default=SchemaVersion.V2)
    venue: Optional[Dict[str, Any]] = None
    organizer: Optional[Dict[str, Any]] = None
    attendees: Optional[int] = None
    status: Optional[str] = None
    last_updated: Optional[datetime] = None

class ValidationPipeline:
    """Pipeline for validating and upgrading event schemas."""
    
    def __init__(self):
        self.performance_monitor = PerformanceMonitor()
    
    @staticmethod
    def validate_event(event_data: Dict[str, Any], target_version: SchemaVersion = SchemaVersion.V2) -> Dict[str, Any]:
        """
        Validate event data and upgrade to target schema version.
        
        Args:
            event_data: Raw event data
            target_version: Target schema version
            
        Returns:
            Validated and upgraded event data
        """
        # Determine current version
        current_version = SchemaVersion(event_data.get('schema_version', SchemaVersion.V1))
        
        # Validate against current version
        if current_version == SchemaVersion.V1:
            event = EventV1(**event_data)
        else:
            event = EventV2(**event_data)
            
        # Upgrade if needed
        if current_version != target_version:
            if target_version == SchemaVersion.V2:
                return ValidationPipeline._upgrade_to_v2(event)
            else:
                return ValidationPipeline._downgrade_to_v1(event)
                
        return event.model_dump()
        
    @staticmethod
    def _upgrade_to_v2(event: EventV1) -> Dict[str, Any]:
        """Upgrade event from V1 to V2 schema."""
        data = event.model_dump()
        data['schema_version'] = SchemaVersion.V2
        data['venue'] = None
        data['organizer'] = None
        data['attendees'] = None
        data['status'] = None
        data['last_updated'] = datetime.utcnow()
        return EventV2(**data).model_dump()
        
    @staticmethod
    def _downgrade_to_v1(event: EventV2) -> Dict[str, Any]:
        """Downgrade event from V2 to V1 schema."""
        data = event.model_dump()
        data['schema_version'] = SchemaVersion.V1
        # Remove V2-specific fields
        for field in ['venue', 'organizer', 'attendees', 'status', 'last_updated']:
            data.pop(field, None)
        return EventV1(**data).model_dump()
    
    def validate_event(self, data: Dict[str, Any], version: SchemaVersion = SchemaVersion.V2) -> Dict[str, Any]:
        """
        Validate event data against schema
        
        Args:
            data: Event data to validate
            version: Schema version to use
            
        Returns:
            Validated and transformed event data
        """
        try:
            with self.performance_monitor.monitor_api_call('event_validation'):
                if version == SchemaVersion.V1:
                    event = EventV1(**data)
                else:
                    event = EventV2(**data)
                return event.dict(exclude_unset=True)
        except Exception as e:
            self.performance_monitor.record_error('event_validation', str(e))
            raise ValueError(f"Validation error: {str(e)}")
    
    def upgrade_schema(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upgrade event data from V1 to V2 schema
        
        Args:
            data: V1 event data
            
        Returns:
            Upgraded V2 event data
        """
        try:
            with self.performance_monitor.monitor_api_call('schema_upgrade'):
                # First validate against V1 schema
                event_v1 = EventV1(**data)
                
                # Convert to V2 format
                v2_data = event_v1.dict(exclude_unset=True)
                v2_data['schema_version'] = SchemaVersion.V2
                
                # Add new V2 fields with defaults
                v2_data.update({
                    'status': 'active',
                    'featured': False,
                    'tags': [],
                    'attendance_mode': 'in_person',
                    'organizer': {}
                })
                
                # Validate against V2 schema
                event_v2 = EventV2(**v2_data)
                return event_v2.dict(exclude_unset=True)
                
        except Exception as e:
            self.performance_monitor.record_error('schema_upgrade', str(e))
            raise ValueError(f"Schema upgrade error: {str(e)}") 