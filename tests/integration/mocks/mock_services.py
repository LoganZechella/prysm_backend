from typing import Dict, Any, List
import pytest
from datetime import datetime
from ..config.test_config import TestConfig

class MockServiceBase:
    """Base class for mock external services"""
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []
        self.responses: Dict[str, Any] = {}
        self.config = TestConfig()

    def record_call(self, method: str, *args, **kwargs):
        """Record service call for verification"""
        self.calls.append({
            'method': method,
            'args': args,
            'kwargs': kwargs,
            'timestamp': datetime.now()
        })

    def set_response(self, method: str, response: Any):
        """Set mock response for service method"""
        self.responses[method] = response

    def get_calls(self, method: str = None) -> List[Dict[str, Any]]:
        """Get recorded calls, optionally filtered by method"""
        if method:
            return [call for call in self.calls if call['method'] == method]
        return self.calls

class MockEventSource(MockServiceBase):
    """Mock external event source"""
    async def get_events(self, *args, **kwargs) -> List[Dict[str, Any]]:
        self.record_call('get_events', *args, **kwargs)
        return self.responses.get('get_events', [])

    async def get_event_details(self, event_id: str) -> Dict[str, Any]:
        self.record_call('get_event_details', event_id)
        return self.responses.get(f'get_event_details_{event_id}', {})

class MockRecommendationService(MockServiceBase):
    """Mock recommendation service"""
    async def get_recommendations(self, user_id: str, *args, **kwargs) -> List[Dict[str, Any]]:
        self.record_call('get_recommendations', user_id, *args, **kwargs)
        return self.responses.get(f'get_recommendations_{user_id}', [])

    async def update_preferences(self, user_id: str, preferences: Dict[str, Any]) -> bool:
        self.record_call('update_preferences', user_id, preferences)
        return self.responses.get(f'update_preferences_{user_id}', True)

class MockLocationService(MockServiceBase):
    """Mock location service"""
    async def geocode(self, address: str) -> Dict[str, Any]:
        self.record_call('geocode', address)
        return self.responses.get(f'geocode_{address}', {})

    async def get_distance(self, origin: Dict[str, float], destination: Dict[str, float]) -> float:
        self.record_call('get_distance', origin, destination)
        return self.responses.get(f'get_distance_{str(origin)}_{str(destination)}', 0.0)

@pytest.fixture
def mock_services():
    """Fixture providing all mock services"""
    return {
        'event_source': MockEventSource(),
        'recommendations': MockRecommendationService(),
        'location': MockLocationService()
    } 