import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from app.utils.schema import Event
from app.utils.location_services import geocode_address, calculate_distance

# Configure root logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def find_events_in_radius(
    events: List[Event],
    center_coords: Dict[str, float],
    radius_km: float = 10.0,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> List[Event]:
    """
    Find events within a specified radius of a location and date range.
    """
    filtered_events = []
    print(f"\nFinding events within {radius_km}km of {center_coords}")
    print(f"Date range: {start_date} to {end_date}")
    
    for event in events:
        # Check if event has valid coordinates
        if event.location.coordinates['lat'] == 0 and event.location.coordinates['lng'] == 0:
            print(f"Skipping event {event.title} - invalid coordinates")
            continue
            
        # Calculate distance from center point
        distance = calculate_distance(center_coords, event.location.coordinates)
        print(f"Event {event.title} distance: {distance}km")
        
        if distance <= radius_km:
            # Check date range if specified
            if start_date and end_date:
                if start_date <= event.start_datetime <= end_date:
                    print(f"Adding event {event.title} - within radius and date range")
                    filtered_events.append(event)
                else:
                    print(f"Skipping event {event.title} - outside date range")
            else:
                print(f"Adding event {event.title} - within radius (no date range)")
                filtered_events.append(event)
        else:
            print(f"Skipping event {event.title} - outside radius")
    
    return filtered_events

def create_location_clusters(
    events: List[Event],
    max_cluster_radius_km: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Group events into geographical clusters to identify hotspots.
    Returns list of clusters with center coordinates and events.
    """
    clusters = []
    processed_events = set()
    
    for event in events:
        if event.event_id in processed_events:
            continue
            
        # Start new cluster
        cluster = {
            'center': event.location.coordinates,
            'events': [event],
            'categories': set(event.categories),
            'avg_price': event.price_info.min_price
        }
        processed_events.add(event.event_id)
        
        # Find nearby events
        for other in events:
            if other.event_id in processed_events:
                continue
                
            distance = calculate_distance(cluster['center'], other.location.coordinates)
            if distance <= max_cluster_radius_km:
                cluster['events'].append(other)
                cluster['categories'].update(other.categories)
                cluster['avg_price'] += other.price_info.min_price
                processed_events.add(other.event_id)
        
        # Calculate cluster metrics
        cluster['avg_price'] /= len(cluster['events'])
        cluster['categories'] = list(cluster['categories'])
        cluster['size'] = len(cluster['events'])
        
        clusters.append(cluster)
    
    return clusters

def get_trip_recommendations(
    events: List[Event],
    city: str,
    state: str = "",
    country: str = "",
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    interests: List[str] = [],
    max_price: float = float('inf'),
    preferred_times: List[str] = []  # e.g. ['morning', 'evening']
) -> Dict[str, Any]:
    """
    Get comprehensive trip recommendations for a city visit.
    """
    # Get city coordinates
    city_coords = geocode_address("", city, state, country)
    if not city_coords:
        raise ValueError(f"Could not geocode city: {city}")
    
    print(f"\nProcessing recommendations:")
    print(f"City coordinates: {city_coords}")
    print(f"Total events before filtering: {len(events)}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Interests: {interests}")
    print(f"Max price: {max_price}")
    print(f"Preferred times: {preferred_times}")
    
    # Find all events in the area
    area_events = find_events_in_radius(
        events,
        city_coords,
        radius_km=20.0,  # Wider radius for city coverage
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"\nEvents after radius filter: {len(area_events)}")
    for event in area_events:
        print(f"- {event.title} ({event.start_datetime}, {event.location.city})")
    
    # Filter by interests if specified
    if interests:
        area_events = [
            event for event in area_events
            if any(interest.lower() in [cat.lower() for cat in event.categories]
                for interest in interests)
        ]
        print(f"\nEvents after interests filter: {len(area_events)}")
        for event in area_events:
            print(f"- {event.title} (categories: {event.categories})")
    
    # Filter by price if specified
    if max_price is not None:
        area_events = [
            event for event in area_events
            if event.price_info.max_price <= max_price
        ]
        print(f"\nEvents after price filter: {len(area_events)}")
        for event in area_events:
            print(f"- {event.title} (price: {event.price_info.max_price})")
    
    # Filter by preferred times if specified
    if preferred_times:
        def is_preferred_time(event: Event) -> bool:
            hour = event.start_datetime.hour
            if 'morning' in preferred_times and 6 <= hour <= 12:
                return True
            if 'afternoon' in preferred_times and 12 < hour <= 17:
                return True
            if 'evening' in preferred_times and 17 < hour <= 23:
                return True
            return False
        
        area_events = [event for event in area_events if is_preferred_time(event)]
        print(f"\nEvents after time filter: {len(area_events)}")
        for event in area_events:
            print(f"- {event.title} (time: {event.start_datetime.hour}:00)")
    
    # Create event clusters
    clusters = create_location_clusters(area_events)
    print(f"\nCreated {len(clusters)} clusters")
    
    # Organize recommendations
    recommendations = {
        'city_coordinates': city_coords,
        'total_events': len(area_events),
        'event_clusters': clusters,
        'top_categories': _get_top_categories(area_events),
        'price_ranges': _get_price_ranges(area_events),
        'daily_distribution': _get_daily_distribution(area_events),
        'suggested_itineraries': _create_itineraries(area_events, start_date, end_date)
    }
    
    return recommendations

def _get_top_categories(events: List[Event], limit: int = 5) -> List[Dict[str, Any]]:
    """Get most common event categories with counts."""
    category_counts = {}
    for event in events:
        for category in event.categories:
            category_counts[category] = category_counts.get(category, 0) + 1
    
    return [
        {'category': cat, 'count': count}
        for cat, count in sorted(
            category_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:limit]
    ]

def _get_price_ranges(events: List[Event]) -> Dict[str, int]:
    """Get distribution of events across price tiers."""
    price_ranges = {'free': 0, 'budget': 0, 'medium': 0, 'premium': 0}
    for event in events:
        price_ranges[event.price_info.price_tier] += 1
    return price_ranges

def _get_daily_distribution(events: List[Event]) -> Dict[str, int]:
    """Get distribution of events across days of the week."""
    days = {
        'Monday': 0, 'Tuesday': 0, 'Wednesday': 0,
        'Thursday': 0, 'Friday': 0, 'Saturday': 0, 'Sunday': 0
    }
    for event in events:
        day = event.start_datetime.strftime('%A')
        days[day] += 1
    return days

def _create_itineraries(
    events: List[Event],
    start_date: Optional[datetime],
    end_date: Optional[datetime]
) -> List[Dict[str, Any]]:
    """Create suggested daily itineraries based on event timing and location."""
    if not start_date or not end_date:
        return []
    
    itineraries = []
    current_date = start_date
    
    while current_date <= end_date:
        # Get events for this day
        day_events = [
            event for event in events
            if event.start_datetime.date() == current_date.date()
        ]
        
        if day_events:
            # Sort events by start time
            day_events.sort(key=lambda x: x.start_datetime)
            
            # Create itinerary ensuring events don't overlap
            itinerary = {
                'date': current_date.date().isoformat(),
                'events': []
            }
            
            last_end_time = None
            for event in day_events:
                if not last_end_time or event.start_datetime >= last_end_time:
                    itinerary['events'].append({
                        'event_id': event.event_id,
                        'title': event.title,
                        'start_time': event.start_datetime.strftime('%H:%M'),
                        'end_time': event.end_datetime.strftime('%H:%M'), # type: ignore
                        'location': event.location.venue_name,
                        'price_tier': event.price_info.price_tier
                    })
                    last_end_time = event.end_datetime
            
            itineraries.append(itinerary)
        
        current_date += timedelta(days=1)
    
    return itineraries 