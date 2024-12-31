from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime, timedelta

def validate_spotify_data(tracks_data: List[Dict[str, Any]], artists_data: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    Validate Spotify data quality.
    
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
            errors.append(f"Missing ID in track: {track.get('name', 'Unknown')}")
        if not track.get('name'):
            errors.append(f"Missing name in track: {track.get('id', 'Unknown')}")
        if not track.get('artists'):
            errors.append(f"Missing artists in track: {track.get('name', 'Unknown')}")
    
    # Validate artists data
    for artist in artists_data:
        if not artist.get('id'):
            errors.append(f"Missing ID in artist: {artist.get('name', 'Unknown')}")
        if not artist.get('name'):
            errors.append(f"Missing name in artist: {artist.get('id', 'Unknown')}")
    
    return len(errors) == 0, errors

def validate_trends_data(topics_data: List[Dict[str, Any]], interest_data: Dict[str, Dict], related_data: Dict[str, List]) -> Tuple[bool, List[str]]:
    """
    Validate Google Trends data quality.
    
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

def validate_event_data(events_data: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    Validate event data quality.
    
    Args:
        events_data: List of event dictionaries
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Check if data is empty
    if not events_data:
        errors.append("Events data is empty")
    
    # Validate event data
    for event in events_data:
        if not event.get('event_id'):
            errors.append(f"Event missing ID: {event.get('title', 'Unknown')}")
        if not event.get('title'):
            errors.append(f"Event missing title: {event.get('event_id', 'Unknown')}")
        if not event.get('start_datetime'):
            errors.append(f"Event missing start time: {event.get('title', 'Unknown')}")
        if not event.get('location', {}).get('venue_name'):
            errors.append(f"Event missing venue: {event.get('title', 'Unknown')}")
    
    return len(errors) == 0, errors

def validate_processed_data(data: Dict[str, Any], schema: Dict[str, type], required_fields: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
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

def check_data_freshness(timestamp: str) -> Tuple[bool, str]:
    """
    Check if data is fresh (less than 24 hours old).
    
    Args:
        timestamp: ISO format timestamp
        
    Returns:
        Tuple of (is_fresh, error_message)
    """
    try:
        data_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        current_time = datetime.now(data_time.tzinfo)
        
        if current_time - data_time > timedelta(hours=24):
            return False, f"Data is too old: {timestamp}"
        return True, ""
    except (ValueError, TypeError) as e:
        return False, f"Invalid timestamp format: {e}"

def check_data_completeness(data: Dict[str, Any], required_fields: List[str]) -> Tuple[bool, List[str]]:
    """
    Check if all required fields are present in the data.
    
    Args:
        data: Data dictionary to check
        required_fields: List of required field names
        
    Returns:
        Tuple of (is_complete, missing_fields)
    """
    missing_fields = [field for field in required_fields if field not in data]
    return len(missing_fields) == 0, missing_fields 