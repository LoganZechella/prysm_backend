import pytest
from datetime import datetime, timedelta
import logging
from unittest.mock import patch
from app.utils.schema import Event, Location, PriceInfo, EventAttributes, SourceInfo
from app.utils.location_recommendations import (
    find_events_in_radius,
    create_location_clusters,
    get_trip_recommendations
)

# Configure logging
logging.basicConfig(level=logging.DEBUG)

@pytest.fixture
def sample_events():
    """Create a list of sample events for testing."""
    base_time = datetime.now()
    events = []
    
    # Event 1: San Francisco downtown
    events.append(Event(
        event_id="1",
        title="Tech Conference",
        description="Annual tech conference",
        start_datetime=base_time + timedelta(days=1, hours=13),  # 1 PM
        end_datetime=base_time + timedelta(days=1, hours=17),    # 5 PM
        location=Location(
            venue_name="Moscone Center",
            address="747 Howard St",
            city="San Francisco",
            state="CA",
            country="USA",
            coordinates={'lat': 37.7847, 'lng': -122.4000}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=100.0,
            max_price=500.0,
            price_tier="premium"
        ),
        categories=["technology", "conference"],
        source=SourceInfo(
            platform="test",
            url="https://example.com/test-event-1",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    # Event 2: Also in SF downtown
    events.append(Event(
        event_id="2",
        title="Evening Jazz",
        description="Jazz performance",
        start_datetime=base_time + timedelta(days=1, hours=19),  # 7 PM
        end_datetime=base_time + timedelta(days=1, hours=22),    # 10 PM
        location=Location(
            venue_name="Jazz Club",
            address="123 Main St",
            city="San Francisco",
            state="CA",
            country="USA",
            coordinates={'lat': 37.7850, 'lng': -122.4010}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=50.0,
            max_price=50.0,
            price_tier="medium"
        ),
        categories=["music", "jazz"],
        source=SourceInfo(
            platform="test",
            url="https://example.com/test-event-2",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    # Event 3: Further away in Oakland
    events.append(Event(
        event_id="3",
        title="Food Festival",
        description="Street food festival",
        start_datetime=base_time + timedelta(days=2, hours=18),  # 6 PM
        end_datetime=base_time + timedelta(days=2, hours=23),    # 11 PM
        location=Location(
            venue_name="Oakland Park",
            address="456 Park Ave",
            city="Oakland",
            state="CA",
            country="USA",
            coordinates={'lat': 37.8044, 'lng': -122.2711}
        ),
        price_info=PriceInfo(
            currency="USD",
            min_price=0.0,
            max_price=0.0,
            price_tier="free"
        ),
        categories=["food", "festival"],
        source=SourceInfo(
            platform="test",
            url="https://example.com/test-event-3",
            last_updated=base_time
        ),
        attributes=EventAttributes()
    ))
    
    return events

@pytest.fixture
def mock_geocoding():
    """Mock the geocoding service to return fixed coordinates for San Francisco."""
    with patch('app.utils.location_recommendations.geocode_address') as mock:
        mock.return_value = {'lat': 37.7749, 'lng': -122.4194}
        yield mock

@pytest.fixture
def mock_distance():
    """Mock the distance calculation to return realistic distances."""
    def calculate_mock_distance(coord1, coord2):
        # Simple approximation for testing
        lat_diff = abs(coord1['lat'] - coord2['lat'])
        lng_diff = abs(coord1['lng'] - coord2['lng'])
        return (lat_diff + lng_diff) * 111  # rough km per degree at this latitude
    
    with patch('app.utils.location_recommendations.calculate_distance', side_effect=calculate_mock_distance) as mock:
        yield mock

def test_find_events_in_radius(sample_events):
    # Test finding events near SF downtown
    sf_coords = {'lat': 37.7749, 'lng': -122.4194}
    nearby_events = find_events_in_radius(sample_events, sf_coords, radius_km=5.0)
    assert len(nearby_events) == 2  # Should find the two SF events
    
    # Test with smaller radius
    very_nearby = find_events_in_radius(sample_events, sf_coords, radius_km=0.1)
    assert len(very_nearby) == 0  # Should find no events within 100m

def test_create_location_clusters(sample_events):
    clusters = create_location_clusters(sample_events, max_cluster_radius_km=1.0)
    assert len(clusters) == 2  # Should create 2 clusters: SF and Oakland
    
    # Check SF cluster
    sf_cluster = next(c for c in clusters if len(c['events']) == 2)
    assert len(sf_cluster['categories']) == 4  # tech, conference, music, jazz
    assert sf_cluster['size'] == 2
    assert 75.0 <= sf_cluster['avg_price'] <= 75.0  # (100 + 50) / 2

def test_get_trip_recommendations(sample_events, mock_geocoding, mock_distance):
    # Use the same base time as the sample events
    base_time = sample_events[0].start_datetime - timedelta(days=1)
    end_time = base_time + timedelta(days=5)  # Extend the range to cover all events
    
    # Print event details for debugging
    print("\nSample events:")
    for event in sample_events:
        print(f"\nEvent: {event.title}")
        print(f"Start time: {event.start_datetime}")
        print(f"Categories: {event.categories}")
        print(f"Price: {event.price_info.max_price}")
        print(f"Location: {event.location.city}, {event.location.state}")
        print(f"Coordinates: {event.location.coordinates}")
    
    recommendations = get_trip_recommendations(
        events=sample_events,
        city="San Francisco",
        state="CA",
        country="USA",
        start_date=base_time,
        end_date=end_time,
        interests=["music", "technology"],
        max_price=200.0,
        preferred_times=["evening"]
    )
    
    # Print recommendations for debugging
    print("\nRecommendations:")
    print(f"Total events: {recommendations['total_events']}")
    print(f"Number of clusters: {len(recommendations['event_clusters'])}")
    if recommendations['event_clusters']:
        for i, cluster in enumerate(recommendations['event_clusters']):
            print(f"\nCluster {i + 1}:")
            print(f"Center: {cluster['center']}")
            print(f"Size: {cluster['size']}")
            print(f"Categories: {cluster['categories']}")
            for event in cluster['events']:
                print(f"- {event.title} ({event.start_datetime.strftime('%H:%M')})")
    
    assert recommendations['total_events'] > 0
    assert len(recommendations['event_clusters']) > 0
    assert len(recommendations['top_categories']) > 0
    assert len(recommendations['suggested_itineraries']) > 0
    
    # Check that filtering works
    for cluster in recommendations['event_clusters']:
        for event in cluster['events']:
            assert any(interest in event.categories for interest in ["music", "technology"])
            assert event.price_info.max_price <= 200.0
            
    # Verify that events are in San Francisco
    assert all(
        event.location.city == "San Francisco"
        for cluster in recommendations['event_clusters']
        for event in cluster['events']
    )
    
    # Check time preferences
    for cluster in recommendations['event_clusters']:
        for event in cluster['events']:
            hour = event.start_datetime.hour
            assert 17 <= hour <= 23  # evening events 