from sqlalchemy import Column, Integer, String, JSON
from app.db.base_class import Base


class SpotifyProfile(Base):
    """Model for storing comprehensive Spotify user profile data including listening history, preferences, and behavior."""
    __tablename__ = 'spotify_profiles'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True, nullable=False)
    profile_data = Column(JSON, nullable=True)  # Stores the full profile data with version history 