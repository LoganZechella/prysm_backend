from typing import List, Optional
from datetime import datetime, timedelta
import random
import uuid
from app.utils.schema import (
    Event, Location, PriceInfo, EventMetadata, 
    EventAttributes, SourceInfo, ImageAnalysis
)
import numpy as np

class TestDataGenerator:
    """Generates test event data for development and testing"""
    
    VENUES = [
        ("The Grand Hall", "San Francisco"),
        ("Blue Note", "New York"),
        ("The Wiltern", "Los Angeles"),
        ("House of Blues", "Chicago"),
        ("9:30 Club", "Washington DC")
    ]
    
    CATEGORIES = [
        "music",
        "concert",
        "theater",
        "comedy",
        "art",
        "food",
        "sports",
        "technology",
        "business",
        "education"
    ]
    
    EVENT_TYPES = [
        "Live Music Performance",
        "Stand-up Comedy Show",
        "Art Exhibition",
        "Tech Conference",
        "Food Festival",
        "Sports Tournament",
        "Theater Production",
        "Business Workshop",
        "Educational Seminar",
        "Cultural Festival"
    ]
    
    def generate_events(self, num_events: int, start_date: Optional[datetime] = None) -> List[Event]:
        """Generate a list of test events
        
        Args:
            num_events: Number of events to generate
            start_date: Optional start date for events (defaults to current date)
        """
        if start_date is None:
            start_date = datetime.now()
        
        events = []
        for _ in range(num_events):
            # Generate random date within next 30 days
            days_offset = random.randint(1, 30)
            hours_offset = random.randint(0, 23)
            minutes_offset = random.choice([0, 15, 30, 45])
            
            event_date = start_date + timedelta(
                days=days_offset,
                hours=hours_offset,
                minutes=minutes_offset
            )
            
            # Select random venue
            venue_name, city = random.choice(self.VENUES)
            
            # Generate random coordinates near the venue
            base_coords = {
                "San Francisco": (37.7749, -122.4194),
                "New York": (40.7128, -74.0060),
                "Los Angeles": (34.0522, -118.2437),
                "Chicago": (41.8781, -87.6298),
                "Washington DC": (38.9072, -77.0369)
            }
            
            base_lat, base_lng = base_coords[city]
            lat = base_lat + random.uniform(-0.01, 0.01)
            lng = base_lng + random.uniform(-0.01, 0.01)
            
            # Generate random categories (1-3)
            num_categories = random.randint(1, 3)
            categories = random.sample(self.CATEGORIES, num_categories)
            
            # Generate random price tier and prices
            price_tier = random.choice(['free', 'budget', 'medium', 'premium'])
            if price_tier == 'free':
                min_price = max_price = 0
            else:
                price_ranges = {
                    'budget': (10, 50),
                    'medium': (50, 150),
                    'premium': (150, 500)
                }
                min_range, max_range = price_ranges[price_tier]
                min_price = round(random.uniform(min_range, max_range/2), 2)
                max_price = round(random.uniform(min_price, max_range), 2)
            
            # Generate event
            event = Event(
                event_id=str(uuid.uuid4()),
                title=f"{random.choice(self.EVENT_TYPES)} at {venue_name}",
                description=f"Join us for an amazing {random.choice(categories)} event at {venue_name}. "
                           f"Experience the best of {city} entertainment!",
                start_datetime=event_date,
                end_datetime=event_date + timedelta(hours=random.randint(2, 4)),
                location=Location(
                    venue_name=venue_name,
                    address=f"123 Main St",
                    city=city,
                    state="CA",
                    country="USA",
                    coordinates={'lat': lat, 'lng': lng}
                ),
                categories=categories,
                tags=categories + [city.lower(), venue_name.lower()],
                price_info=PriceInfo(
                    currency="USD",
                    min_price=min_price,
                    max_price=max_price,
                    price_tier=price_tier
                ),
                attributes=EventAttributes(),
                source=SourceInfo(
                    platform="test_generator",
                    url=f"https://example.com/events/{uuid.uuid4()}",
                    last_updated=datetime.now()
                ),
                metadata=EventMetadata(
                    popularity_score=random.uniform(0, 1),
                    social_mentions=random.randint(0, 1000),
                    verified=random.choice([True, False])
                )
            )
            
            events.append(event)
        
        return events 