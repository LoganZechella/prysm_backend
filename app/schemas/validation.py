"""
Data validation schemas with versioning support.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, validator, root_validator
import re
from app.monitoring.performance import PerformanceMonitor

class SchemaVersion(str, Enum):
    V1 = "1.0"
    V2 = "2.0"

class Category(BaseModel):
    """Event category schema"""
    name: str
    parent: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)
    
    @validator('name')
    def validate_category_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Category name cannot be empty")
        if not re.match(r'^[a-zA-Z0-9\s\-_]+$', v):
            raise ValueError("Category name contains invalid characters")
        return v

class Location(BaseModel):
    """Event location schema"""
    name: str
    address: str
    city: str
    state: Optional[str]
    country: str
    postal_code: Optional[str]
    latitude: Optional[float] = Field(ge=-90.0, le=90.0)
    longitude: Optional[float] = Field(ge=-180.0, le=180.0)
    
    @validator('postal_code')
    def validate_postal_code(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().upper()
        if not re.match(r'^[A-Z0-9\-\s]+$', v):
            raise ValueError("Invalid postal code format")
        return v

class Price(BaseModel):
    """Event price schema"""
    amount: float = Field(ge=0.0)
    currency: str
    tier: Optional[str]
    
    @validator('currency')
    def validate_currency(cls, v: str) -> str:
        v = v.strip().upper()
        if not re.match(r'^[A-Z]{3}$', v):
            raise ValueError("Currency must be a 3-letter ISO code")
        return v

class EventBase(BaseModel):
    """Base event schema with common fields"""
    title: str
    description: str
    start_date: datetime
    end_date: Optional[datetime]
    categories: List[Category]
    location: Location
    prices: List[Price]
    source: str
    external_id: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('title')
    def validate_title(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Title cannot be empty")
        if len(v) > 200:
            raise ValueError("Title is too long (max 200 characters)")
        return v
    
    @validator('description')
    def validate_description(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Description cannot be empty")
        return v
    
    @validator('end_date')
    def validate_end_date(cls, v: Optional[datetime], values: Dict[str, Any]) -> Optional[datetime]:
        if v and 'start_date' in values and v < values['start_date']:
            raise ValueError("End date must be after start date")
        return v
    
    @validator('external_id')
    def validate_external_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("External ID cannot be empty")
        if not re.match(r'^[a-zA-Z0-9\-_]+$', v):
            raise ValueError("External ID contains invalid characters")
        return v

class EventV1(EventBase):
    """Version 1.0 of event schema"""
    schema_version: str = Field(default=SchemaVersion.V1, const=True)

class EventV2(EventBase):
    """Version 2.0 of event schema with additional fields"""
    schema_version: str = Field(default=SchemaVersion.V2, const=True)
    status: str = Field(default="active")
    featured: bool = Field(default=False)
    tags: List[str] = Field(default_factory=list)
    attendance_mode: str = Field(default="in_person")
    capacity: Optional[int] = Field(ge=0)
    organizer: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('status')
    def validate_status(cls, v: str) -> str:
        valid_statuses = {'active', 'cancelled', 'postponed', 'rescheduled'}
        v = v.lower()
        if v not in valid_statuses:
            raise ValueError(f"Invalid status. Must be one of: {', '.join(valid_statuses)}")
        return v
    
    @validator('attendance_mode')
    def validate_attendance_mode(cls, v: str) -> str:
        valid_modes = {'in_person', 'online', 'hybrid'}
        v = v.lower()
        if v not in valid_modes:
            raise ValueError(f"Invalid attendance mode. Must be one of: {', '.join(valid_modes)}")
        return v
    
    @validator('tags')
    def validate_tags(cls, v: List[str]) -> List[str]:
        return [tag.strip().lower() for tag in v if tag.strip()]

class ValidationPipeline:
    """Pipeline for validating and transforming event data"""
    
    def __init__(self):
        self.performance_monitor = PerformanceMonitor()
    
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