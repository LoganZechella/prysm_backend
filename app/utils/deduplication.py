"""
Utility functions for detecting and handling duplicate events.
"""

from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
from difflib import SequenceMatcher
import re
import logging
from app.models.event import EventModel

logger = logging.getLogger(__name__)

def calculate_title_similarity(title1: str, title2: str) -> float:
    """
    Calculate similarity between two event titles.
    
    Args:
        title1: First event title
        title2: Second event title
        
    Returns:
        Similarity score between 0 and 1
    """
    try:
        # Normalize titles
        t1 = _normalize_title(title1)
        t2 = _normalize_title(title2)
        
        # Calculate similarity using SequenceMatcher
        matcher = SequenceMatcher(None, t1, t2)
        return matcher.ratio()
        
    except Exception as e:
        logger.error(f"Error calculating title similarity: {str(e)}")
        return 0.0

def calculate_time_proximity(time1: datetime, time2: datetime) -> float:
    """
    Calculate proximity score between two event times.
    
    Args:
        time1: First event time
        time2: Second event time
        
    Returns:
        Proximity score between 0 and 1 (1 means same time)
    """
    try:
        # Calculate time difference in hours
        diff = abs((time1 - time2).total_seconds()) / 3600
        
        # Score decreases as difference increases
        # Score is 1.0 if difference is 0 hours
        # Score is 0.5 if difference is 24 hours
        # Score approaches 0 as difference increases
        return 1.0 / (1.0 + diff/24.0)
        
    except Exception as e:
        logger.error(f"Error calculating time proximity: {str(e)}")
        return 0.0

def calculate_category_overlap(categories1: List[str], categories2: List[str]) -> float:
    """
    Calculate overlap between two sets of categories.
    
    Args:
        categories1: First list of categories
        categories2: Second list of categories
        
    Returns:
        Overlap score between 0 and 1
    """
    try:
        if not categories1 or not categories2:
            return 0.0
            
        # Convert to sets and calculate Jaccard similarity
        set1 = set(c.lower() for c in categories1)
        set2 = set(c.lower() for c in categories2)
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
        
    except Exception as e:
        logger.error(f"Error calculating category overlap: {str(e)}")
        return 0.0

def calculate_description_similarity(desc1: str, desc2: str) -> float:
    """
    Calculate similarity between two event descriptions.
    
    Args:
        desc1: First event description
        desc2: Second event description
        
    Returns:
        Similarity score between 0 and 1
    """
    try:
        if not desc1 or not desc2:
            return 0.0
            
        # Normalize descriptions
        d1 = _normalize_text(desc1)
        d2 = _normalize_text(desc2)
        
        # Calculate similarity using SequenceMatcher
        matcher = SequenceMatcher(None, d1, d2)
        return matcher.ratio()
        
    except Exception as e:
        logger.error(f"Error calculating description similarity: {str(e)}")
        return 0.0

def calculate_duplicate_score(event1: Dict[str, Any], event2: Dict[str, Any]) -> float:
    """
    Calculate overall duplicate score between two events.
    
    Args:
        event1: First event dictionary
        event2: Second event dictionary
        
    Returns:
        Duplicate score between 0 and 1
    """
    try:
        # Calculate individual similarity scores
        title_sim = calculate_title_similarity(
            event1.get('title', ''),
            event2.get('title', '')
        )
        
        time_sim = calculate_time_proximity(
            event1.get('start_datetime', datetime.max),
            event2.get('start_datetime', datetime.min)
        )
        
        cat_sim = calculate_category_overlap(
            event1.get('categories', []),
            event2.get('categories', [])
        )
        
        desc_sim = calculate_description_similarity(
            event1.get('description', ''),
            event2.get('description', '')
        )
        
        # Weight the scores
        # Title similarity is most important
        # Time proximity is second most important
        # Category and description similarities are less important
        weights = {
            'title': 0.4,
            'time': 0.3,
            'category': 0.2,
            'description': 0.1
        }
        
        score = (
            weights['title'] * title_sim +
            weights['time'] * time_sim +
            weights['category'] * cat_sim +
            weights['description'] * desc_sim
        )
        
        return score
        
    except Exception as e:
        logger.error(f"Error calculating duplicate score: {str(e)}")
        return 0.0

def find_duplicate_events(events: List[Dict[str, Any]], threshold: float = 0.8) -> List[Tuple[Dict[str, Any], Dict[str, Any], float]]:
    """
    Find potential duplicate events.
    
    Args:
        events: List of event dictionaries
        threshold: Minimum similarity threshold
        
    Returns:
        List of tuples (event1, event2, similarity_score)
    """
    try:
        duplicates = []
        
        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                score = calculate_duplicate_score(events[i], events[j])
                if score >= threshold:
                    duplicates.append((events[i], events[j], score))
        
        # Sort by score descending
        duplicates.sort(key=lambda x: x[2], reverse=True)
        return duplicates
        
    except Exception as e:
        logger.error(f"Error finding duplicate events: {str(e)}")
        return []

def merge_duplicate_events(event1: Dict[str, Any], event2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge two duplicate events into one.
    
    Args:
        event1: First event dictionary
        event2: Second event dictionary
        
    Returns:
        Merged event dictionary
    """
    try:
        # Start with event1 as base
        merged = event1.copy()
        
        # Merge categories
        categories = set(event1.get('categories', []))
        categories.update(event2.get('categories', []))
        merged['categories'] = list(categories)
        
        # Take the longer description
        desc1 = event1.get('description', '')
        desc2 = event2.get('description', '')
        merged['description'] = desc1 if len(desc1) >= len(desc2) else desc2
        
        # Merge source information
        sources = []
        if 'source' in event1:
            sources.append(event1['source'])
        if 'source' in event2:
            sources.append(event2['source'])
        if sources:
            merged['sources'] = sources
        
        # Take the earlier start time
        start1 = event1.get('start_datetime')
        start2 = event2.get('start_datetime')
        if start1 and start2:
            merged['start_datetime'] = min(start1, start2)
        
        # Take the later end time
        end1 = event1.get('end_datetime')
        end2 = event2.get('end_datetime')
        if end1 and end2:
            merged['end_datetime'] = max(end1, end2)
        
        # Merge price information
        price1 = event1.get('price_info', {})
        price2 = event2.get('price_info', {})
        if price1 or price2:
            merged['price_info'] = _merge_price_info(price1, price2)
        
        return merged
        
    except Exception as e:
        logger.error(f"Error merging duplicate events: {str(e)}")
        return event1

def deduplicate_events(events: List[Dict[str, Any]], threshold: float = 0.8) -> List[Dict[str, Any]]:
    """
    Remove and merge duplicate events from a list.
    
    Args:
        events: List of event dictionaries
        threshold: Minimum similarity threshold
        
    Returns:
        List of unique events with duplicates merged
    """
    try:
        if not events:
            return []
            
        # Find all duplicates
        duplicates = find_duplicate_events(events, threshold)
        
        # Track which events have been merged
        merged_ids = set()
        result = []
        
        # Process duplicates in order of similarity score
        for event1, event2, score in duplicates:
            id1 = event1.get('id')
            id2 = event2.get('id')
            
            # Skip if either event has already been merged
            if id1 in merged_ids or id2 in merged_ids:
                continue
                
            # Merge the duplicates
            merged = merge_duplicate_events(event1, event2)
            result.append(merged)
            merged_ids.add(id1)
            merged_ids.add(id2)
        
        # Add remaining non-duplicate events
        for event in events:
            if event.get('id') not in merged_ids:
                result.append(event)
        
        return result
        
    except Exception as e:
        logger.error(f"Error deduplicating events: {str(e)}")
        return events

def _normalize_title(title: str) -> str:
    """Normalize title for comparison."""
    # Convert to lowercase
    text = title.lower()
    
    # Remove special characters and extra whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    text = ' '.join(text.split())
    
    # Remove common words that don't add meaning
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with'}
    words = text.split()
    words = [w for w in words if w not in stop_words]
    
    return ' '.join(words)

def _normalize_text(text: str) -> str:
    """Normalize text for comparison."""
    # Convert to lowercase
    text = text.lower()
    
    # Remove special characters and extra whitespace
    text = re.sub(r'[^\w\s]', ' ', text)
    text = ' '.join(text.split())
    
    return text

def _merge_price_info(price1: Dict[str, Any], price2: Dict[str, Any]) -> Dict[str, Any]:
    """Merge price information from two events."""
    if not price1:
        return price2
    if not price2:
        return price1
        
    merged = {}
    
    # Take the lower minimum price
    min1 = price1.get('min_price')
    min2 = price2.get('min_price')
    if min1 is not None and min2 is not None:
        merged['min_price'] = min(min1, min2)
    elif min1 is not None:
        merged['min_price'] = min1
    elif min2 is not None:
        merged['min_price'] = min2
    
    # Take the higher maximum price
    max1 = price1.get('max_price')
    max2 = price2.get('max_price')
    if max1 is not None and max2 is not None:
        merged['max_price'] = max(max1, max2)
    elif max1 is not None:
        merged['max_price'] = max1
    elif max2 is not None:
        merged['max_price'] = max2
    
    # Prefer non-null currency
    merged['currency'] = price1.get('currency') or price2.get('currency')
    
    return merged 