import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional

def validate_spotify_data(tracks_data: List[Dict], artists_data: List[Dict]) -> Tuple[bool, List[str]]:
    """
    Validate Spotify data for quality and completeness.
    
    Args:
        tracks_data: List of track dictionaries
        artists_data: List of artist dictionaries
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Check if data is empty
    if not tracks_data:
        errors.append("Tracks data is empty")
    if not artists_data:
        errors.append("Artists data is empty")
    
    # Validate tracks data
    for track in tracks_data:
        if not track.get('id'):
            errors.append(f"Track missing ID: {track.get('name', 'Unknown')}")
        if not track.get('name'):
            errors.append(f"Track missing name: {track.get('id', 'Unknown')}")
        if not track.get('artists'):
            errors.append(f"Track missing artists: {track.get('name', 'Unknown')}")
    
    # Validate artists data
    for artist in artists_data:
        if not artist.get('id'):
            errors.append(f"Artist missing ID: {artist.get('name', 'Unknown')}")
        if not artist.get('name'):
            errors.append(f"Artist missing name: {artist.get('id', 'Unknown')}")
    
    return len(errors) == 0, errors

def validate_trends_data(
    topics_data: List[Dict],
    interest_data: Dict[str, Dict],
    related_data: Dict[str, List]
) -> Tuple[bool, List[str]]:
    """
    Validate Google Trends data for quality and completeness.
    
    Args:
        topics_data: List of topic dictionaries
        interest_data: Dictionary of interest over time data
        related_data: Dictionary of related topics
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Check if data is empty
    if not topics_data:
        errors.append("Topics data is empty")
    if not interest_data:
        errors.append("Interest data is empty")
    if not related_data:
        errors.append("Related topics data is empty")
    
    # Validate topics data
    topic_names = set()
    for topic in topics_data:
        if not topic.get('topic'):
            errors.append("Topic missing name")
        else:
            topic_names.add(topic['topic'])
            
        if not topic.get('interest_over_time'):
            errors.append(f"Topic missing interest data: {topic.get('topic', 'Unknown')}")
    
    # Validate interest data matches topics
    for topic in interest_data.keys():
        if not topic_names or topic not in topic_names:
            errors.append(f"Unknown topic in interest data: {topic}")
    
    return len(errors) == 0, errors

def validate_linkedin_data(
    profile_data: Dict,
    connections_data: Dict,
    activity_data: Dict
) -> Tuple[bool, List[str]]:
    """
    Validate LinkedIn data for quality and completeness.
    
    Args:
        profile_data: User profile dictionary
        connections_data: Connections data dictionary
        activity_data: Activity data dictionary
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Validate profile data
    if not profile_data.get('id'):
        errors.append("Profile missing ID")
    if not profile_data.get('firstName', {}).get('localized', {}).get('en_US'):
        errors.append("Profile missing first name")
    if not profile_data.get('lastName', {}).get('localized', {}).get('en_US'):
        errors.append("Profile missing last name")
    
    # Validate connections data
    if not connections_data.get('elements'):
        errors.append("Connections data is empty")
    else:
        for connection in connections_data['elements']:
            if not connection.get('id'):
                errors.append("Connection missing ID")
            if not connection.get('firstName', {}).get('localized', {}).get('en_US'):
                errors.append(f"Connection missing first name: {connection.get('id', 'Unknown')}")
    
    # Validate activity data
    if not activity_data.get('elements'):
        errors.append("Activity data is empty")
    else:
        for activity in activity_data['elements']:
            if not activity.get('id'):
                errors.append("Activity missing ID")
            if not activity.get('type'):
                errors.append(f"Activity missing type: {activity.get('id', 'Unknown')}")
    
    return len(errors) == 0, errors

def check_data_freshness(timestamp: str, max_age_hours: int = 24) -> Tuple[bool, Optional[str]]:
    """
    Check if data is within acceptable age range.
    
    Args:
        timestamp: ISO format timestamp string
        max_age_hours: Maximum acceptable age in hours
        
    Returns:
        Tuple of (is_fresh, error_message)
    """
    try:
        data_time = datetime.fromisoformat(timestamp)
        age = datetime.now() - data_time
        
        if age > timedelta(hours=max_age_hours):
            return False, f"Data is too old: {age.total_seconds() / 3600:.1f} hours"
        
        return True, None
    except ValueError:
        return False, "Invalid timestamp format"

def check_data_completeness(data: Dict, required_fields: List[str]) -> Tuple[bool, List[str]]:
    """
    Check if all required fields are present in the data.
    
    Args:
        data: Dictionary to check
        required_fields: List of required field names
        
    Returns:
        Tuple of (is_complete, missing_fields)
    """
    missing_fields = []
    
    for field in required_fields:
        if field not in data or data[field] is None:
            missing_fields.append(field)
    
    return len(missing_fields) == 0, missing_fields

def validate_processed_data(
    data: Dict,
    schema: Dict[str, type],
    required_fields: Optional[List[str]] = None
) -> Tuple[bool, List[str]]:
    """
    Validate processed data against a schema.
    
    Args:
        data: Dictionary to validate
        schema: Dictionary mapping field names to expected types
        required_fields: Optional list of required field names
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Check required fields if specified
    if required_fields:
        is_complete, missing_fields = check_data_completeness(data, required_fields)
        if not is_complete:
            errors.extend([f"Missing required field: {field}" for field in missing_fields])
    
    # Validate field types
    for field, expected_type in schema.items():
        if field in data and data[field] is not None:
            if not isinstance(data[field], expected_type):
                errors.append(
                    f"Invalid type for {field}: expected {expected_type.__name__}, "
                    f"got {type(data[field]).__name__}"
                )
    
    return len(errors) == 0, errors 