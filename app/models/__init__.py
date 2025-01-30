"""Models package initialization."""

from app.database import Base
from app.models.event import EventModel
from app.models.preferences import UserPreferences, UserPreferencesBase, LocationPreference
from app.models.traits import Traits
from app.models.oauth import OAuthToken
from app.models.profile import Profile
from app.models.event_score import EventScore
from app.models.feedback import UserFeedback, ImplicitFeedback

# Ensure all models are registered with Base
__all__ = [
    'Base',
    'EventModel',
    'UserPreferences',
    'UserPreferencesBase',
    'LocationPreference',
    'Traits',
    'OAuthToken',
    'Profile',
    'EventScore',
    'UserFeedback',
    'ImplicitFeedback'
]

# Register models with Base
Base.registry.configure()
