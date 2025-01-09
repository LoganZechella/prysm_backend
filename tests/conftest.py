import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.database import Base
from app.models import Event, UserPreferences
import os

# Test database URL
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/events_test")

pytest_plugins = ('pytest_asyncio',)

@pytest.fixture(scope="session")
def engine():
    """Create a test database engine."""
    engine = create_engine(TEST_DATABASE_URL)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)

@pytest.fixture(scope="session")
def TestingSessionLocal(engine):
    """Create a test database session factory."""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal

@pytest.fixture
def db_session(TestingSessionLocal):
    """Create a test database session."""
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.rollback()
        session.close()

@pytest.fixture
def sample_events(db_session):
    """Create sample events for testing."""
    from datetime import datetime, timedelta
    
    now = datetime.utcnow()
    events = [
        Event(
            title="Rock Concert in the Park",
            description="Live rock music festival featuring local bands",
            categories=["music", "entertainment", "outdoor"],
            start_time=now + timedelta(days=3),
            end_time=now + timedelta(days=3, hours=4),
            location={"lat": 40.7128, "lng": -74.0060},  # NYC
            price_info={
                "type": "fixed",
                "amount": 50.0,
                "currency": "USD"
            },
            source="eventbrite",
            source_id="evt_123",
            url="https://example.com/event1",
            view_count=500
        ),
        Event(
            title="Tech Conference 2024",
            description="Annual technology conference with workshops",
            categories=["technology", "education", "networking"],
            start_time=now + timedelta(days=10),
            end_time=now + timedelta(days=12),
            location={"lat": 37.7749, "lng": -122.4194},  # SF
            price_info={
                "type": "range",
                "min_amount": 200.0,
                "max_amount": 500.0,
                "currency": "USD"
            },
            source="eventbrite",
            source_id="evt_456",
            url="https://example.com/event2",
            view_count=1000
        ),
        Event(
            title="Free Yoga in the Park",
            description="Morning yoga session for all levels",
            categories=["wellness", "outdoor"],
            start_time=now + timedelta(days=1),
            end_time=now + timedelta(days=1, hours=2),
            location={"lat": 40.7829, "lng": -73.9654},  # Central Park
            price_info={
                "type": "fixed",
                "amount": 0.0,
                "currency": "USD"
            },
            source="eventbrite",
            source_id="evt_789",
            url="https://example.com/event3",
            view_count=100
        )
    ]
    
    for event in events:
        db_session.add(event)
    db_session.commit()
    
    return events

@pytest.fixture
def sample_preferences(db_session):
    """Create sample user preferences for testing."""
    preferences = UserPreferences(
        user_id="test_user",
        preferred_categories=["music", "outdoor"],
        excluded_categories=["technology"],
        min_price=0.0,
        max_price=100.0,
        preferred_location={
            "lat": 40.7128,
            "lng": -74.0060,
            "max_distance_km": 50
        },
        preferred_days=["Monday", "Friday", "Saturday"],
        preferred_times=["morning", "evening"]
    )
    
    db_session.add(preferences)
    db_session.commit()
    
    return preferences 

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close() 