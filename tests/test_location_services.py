import pytest
from app.utils.location_services import geocode_address, calculate_distance

def test_geocode_address():
    # Test with a well-known address
    coords = geocode_address(
        address="1 Infinite Loop",
        city="Cupertino",
        state="CA",
        country="USA"
    )
    
    assert coords is not None
    assert 'lat' in coords
    assert 'lng' in coords
    # Apple HQ coordinates (approximately)
    assert 37.33 <= coords['lat'] <= 37.34
    assert -122.04 <= coords['lng'] <= -122.02

def test_calculate_distance():
    # Test distance between two known points
    point1 = {'lat': 37.7749, 'lng': -122.4194}  # San Francisco
    point2 = {'lat': 34.0522, 'lng': -118.2437}  # Los Angeles
    
    distance = calculate_distance(point1, point2)
    
    assert distance > 0
    # Distance should be around 600km (driving distance)
    assert 500 <= distance <= 700

def test_geocode_invalid_address():
    # Test with an invalid address
    coords = geocode_address(
        address="Invalid Street 123456",
        city="Nonexistent City",
        state="XX",
        country="Nowhere"
    )
    
    assert coords is None 