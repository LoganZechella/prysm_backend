import logging
from typing import List, Tuple, Dict, Set, Any, Optional
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from dataclasses import dataclass
from app.schemas.event import Event
from app.utils.location_services import calculate_distance
from app.services.location_recommendations import LocationService

logger = logging.getLogger(__name__)

@dataclass
class DuplicateScore:
    """Represents how likely two events are duplicates."""
    title_similarity: float  # 0-1 score for title match
    time_proximity: float   # 0-1 score for how close in time
    location_proximity: float  # 0-1 score for physical distance
    category_overlap: float  # 0-1 score for matching categories
    description_similarity: float  # 0-1 score for description match
    combined_score: float  # Weighted average of all scores

class DeduplicationService:
    """Service for deduplicating events."""
    
    def __init__(self):
        """Initialize deduplication service."""
        self.location_service = LocationService()
        
    def deduplicate_events(
        self,
        events: List[Dict[str, Any]],
        similarity_threshold: float = 0.8,
        distance_threshold: float = 0.1  # 100 meters
    ) -> List[Dict[str, Any]]:
        """
        Remove duplicate events based on title and location similarity.
        
        Args:
            events: List of events to deduplicate
            similarity_threshold: Minimum title similarity to consider events duplicates
            distance_threshold: Maximum distance in km to consider same location
            
        Returns:
            Deduplicated list of events
        """
        try:
            unique_events = []
            
            for event in events:
                is_duplicate = False
                
                for unique_event in unique_events:
                    # Check title similarity
                    if self._calculate_title_similarity(
                        event.get("title", ""),
                        unique_event.get("title", "")
                    ) >= similarity_threshold:
                        # If titles are similar, check location
                        if self._are_locations_same(
                            event,
                            unique_event,
                            distance_threshold
                        ):
                            # Keep event with more complete information
                            if self._is_more_complete(event, unique_event):
                                unique_events.remove(unique_event)
                                unique_events.append(event)
                            is_duplicate = True
                            break
                
                if not is_duplicate:
                    unique_events.append(event)
            
            return unique_events
            
        except Exception as e:
            logger.error(f"Error deduplicating events: {str(e)}")
            return events
            
    def _calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity score between two event titles."""
        if not title1 or not title2:
            return 0.0
            
        # Remove common prefixes/suffixes that don't affect similarity
        prefixes = ["the ", "a ", "an "]
        suffixes = [" event", " show", " party"]
        
        title1 = title1.lower()
        title2 = title2.lower()
        
        for prefix in prefixes:
            title1 = title1[len(prefix):] if title1.startswith(prefix) else title1
            title2 = title2[len(prefix):] if title2.startswith(prefix) else title2
            
        for suffix in suffixes:
            title1 = title1[:-len(suffix)] if title1.endswith(suffix) else title1
            title2 = title2[:-len(suffix)] if title2.endswith(suffix) else title2
            
        return SequenceMatcher(None, title1, title2).ratio()
        
    def _are_locations_same(
        self,
        event1: Dict[str, Any],
        event2: Dict[str, Any],
        distance_threshold: float
    ) -> bool:
        """Check if two events are at the same location."""
        try:
            # Get coordinates for both events
            coords1 = {
                "lat": event1.get("venue_lat") or event1.get("lat"),
                "lon": event1.get("venue_lon") or event1.get("lng") or event1.get("lon")
            }
            
            coords2 = {
                "lat": event2.get("venue_lat") or event2.get("lat"),
                "lon": event2.get("venue_lon") or event2.get("lng") or event2.get("lon")
            }
            
            # If both events have coordinates, check distance
            if all(coords1.values()) and all(coords2.values()):
                distance = self.location_service.calculate_distance(coords1, coords2)
                return distance <= distance_threshold
                
            # If no coordinates, check venue names and cities
            return (
                event1.get("venue_name") == event2.get("venue_name") and
                event1.get("venue_city") == event2.get("venue_city") and
                event1.get("venue_state") == event2.get("venue_state")
            )
            
        except Exception as e:
            logger.error(f"Error comparing event locations: {str(e)}")
            return False
            
    def _is_more_complete(self, event1: Dict[str, Any], event2: Dict[str, Any]) -> bool:
        """Check if event1 has more complete information than event2."""
        score1 = 0
        score2 = 0
        
        # Fields to check for completeness
        fields = [
            "description",
            "image_url",
            "price_info",
            "categories",
            "organizer_name",
            "venue_name",
            "venue_lat",
            "venue_lon"
        ]
        
        for field in fields:
            if event1.get(field):
                score1 += 1
            if event2.get(field):
                score2 += 1
                
        return score1 > score2

def calculate_time_proximity(time1: datetime, time2: datetime, max_delta: timedelta = timedelta(hours=24)) -> float:
    """
    Calculate how close two event times are.
    Returns 1 for same time, 0 for times further apart than max_delta.
    """
    delta = abs(time1 - time2)
    if delta > max_delta:
        return 0.0
    # Linear decay with a boost for very close times
    base_score = 1.0 - (delta.total_seconds() / max_delta.total_seconds())
    return min(1.0, base_score * 1.2) if base_score > 0.8 else base_score

def calculate_category_overlap(categories1: List[str], categories2: List[str]) -> float:
    """
    Calculate overlap between two sets of categories.
    Returns ratio of common categories to total unique categories.
    """
    if not categories1 or not categories2:
        return 0.0
        
    set1 = set(categories1)
    set2 = set(categories2)
    
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    
    return intersection / union if union > 0 else 0.0

def calculate_description_similarity(desc1: str, desc2: str) -> float:
    """
    Calculate similarity between event descriptions.
    Returns a score between 0 and 1.
    """
    # Clean and normalize descriptions
    d1 = desc1.lower().strip() if desc1 else ""
    d2 = desc2.lower().strip() if desc2 else ""
    
    if not d1 or not d2:
        return 0.0
    
    # Use SequenceMatcher for fuzzy string matching
    return SequenceMatcher(None, d1, d2).ratio()

def calculate_duplicate_score(event1: Event, event2: Event) -> DuplicateScore:
    """
    Calculate comprehensive duplicate score between two events.
    Returns DuplicateScore with individual and combined metrics.
    """
    # Calculate individual similarity scores
    title_sim = calculate_title_similarity(event1.title, event2.title)
    
    time_prox = calculate_time_proximity(
        event1.start_datetime,
        event2.start_datetime
    )
    
    # Calculate location proximity if coordinates available
    location_prox = 0.0
    if (event1.location.coordinates and event2.location.coordinates and
        event1.location.coordinates['lat'] != 0 and event1.location.coordinates['lng'] != 0 and
        event2.location.coordinates['lat'] != 0 and event2.location.coordinates['lng'] != 0):
        
        distance = calculate_distance(
            event1.location.coordinates,
            event2.location.coordinates
        )
        # Score decreases as distance increases, up to 10km
        location_prox = max(0.0, 1.0 - (distance / 10.0))
    
    cat_overlap = calculate_category_overlap(
        event1.categories,
        event2.categories
    )
    
    desc_sim = calculate_description_similarity(
        event1.description,
        event2.description
    )
    
    # Calculate weighted combined score with adjusted weights
    # Increase weight of title and time proximity
    combined = (
        0.35 * title_sim +
        0.30 * time_prox +
        0.20 * location_prox +
        0.10 * cat_overlap +
        0.05 * desc_sim
    )
    
    # Boost score if title and time are very similar
    if title_sim > 0.8 and time_prox > 0.8:
        combined = min(1.0, combined * 1.1)
    
    return DuplicateScore(
        title_similarity=title_sim,
        time_proximity=time_prox,
        location_proximity=location_prox,
        category_overlap=cat_overlap,
        description_similarity=desc_sim,
        combined_score=combined
    )

def find_duplicate_events(events: List[Event], threshold: float = 0.8) -> List[Tuple[Event, Event, float]]:
    """
    Find potential duplicate events in a list.
    Returns list of (event1, event2, score) tuples where score > threshold.
    """
    duplicates = []
    
    # Compare each pair of events
    for i, event1 in enumerate(events):
        for event2 in events[i+1:]:
            # Skip if events are from same source
            if event1.source.platform == event2.source.platform:
                continue
                
            score = calculate_duplicate_score(event1, event2)
            
            if score.combined_score >= threshold:
                duplicates.append((event1, event2, score.combined_score))
    
    return sorted(duplicates, key=lambda x: x[2], reverse=True)

def merge_duplicate_events(event1: Event, event2: Event) -> Event:
    """
    Merge two duplicate events into a single event.
    Uses the most complete and reliable information from each event.
    """
    # Start with the event that has more complete information
    base_event = event1 if len(event1.description) > len(event2.description) else event2
    other_event = event2 if base_event == event1 else event1
    
    # Create merged event using model_dump instead of dict
    merged = Event(**base_event.model_dump())
    
    # Merge source information
    merged.source.platform = f"{base_event.source.platform}+{other_event.source.platform}"
    
    # Merge categories (unique)
    merged.categories = list(set(base_event.categories + other_event.categories))
    
    # Use most specific location information
    if not base_event.location.coordinates or (
        base_event.location.coordinates['lat'] == 0 and
        base_event.location.coordinates['lng'] == 0):
        merged.location.coordinates = other_event.location.coordinates
    
    # Merge price information (use wider range)
    if base_event.price_info is not None and other_event.price_info is not None:
        if merged.price_info is None:
            merged.price_info = base_event.price_info
        merged.price_info.min_price = min(
            base_event.price_info.min_price,
            other_event.price_info.min_price
        )
        merged.price_info.max_price = max(
            base_event.price_info.max_price,
            other_event.price_info.max_price
        )
    elif other_event.price_info is not None:
        merged.price_info = other_event.price_info
    
    return merged

def deduplicate_events(events: List[Event], threshold: float = 0.8) -> List[Event]:
    """
    Remove duplicate events from a list.
    Returns deduplicated list of events.
    """
    if not events:
        return []
    
    # Find all duplicates above threshold
    duplicates = find_duplicate_events(events, threshold)
    
    # Track which events have been merged
    merged_ids = set()
    deduplicated = []
    
    # Process duplicates in order of confidence
    for event1, event2, score in duplicates:
        if event1.event_id not in merged_ids and event2.event_id not in merged_ids:
            # Merge the duplicates
            merged = merge_duplicate_events(event1, event2)
            deduplicated.append(merged)
            merged_ids.add(event1.event_id)
            merged_ids.add(event2.event_id)
    
    # Add all non-duplicate events
    for event in events:
        if event.event_id not in merged_ids:
            deduplicated.append(event)
    
    return deduplicated 