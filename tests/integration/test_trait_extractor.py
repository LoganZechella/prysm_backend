"""Integration tests for the trait extractor service."""
import pytest
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.traits import Traits
from app.services.trait_extractor import TraitExtractor
from app.services.spotify import SpotifyService
from app.core.cache import CacheManager


@pytest.fixture
async def spotify_service(test_db: Session) -> SpotifyService:
    """Create a Spotify service instance for testing."""
    from app.models.oauth import OAuthToken
    
    # Create a test token using real credentials
    token = OAuthToken(
        user_id="test_user",
        provider="spotify",
        access_token=settings.SPOTIFY_TEST_ACCESS_TOKEN,
        refresh_token=settings.SPOTIFY_TEST_REFRESH_TOKEN,
        expires_at=datetime.utcnow() + timedelta(hours=1),
        client_id=settings.SPOTIFY_CLIENT_ID,
        client_secret=settings.SPOTIFY_CLIENT_SECRET,
        redirect_uri=settings.SPOTIFY_REDIRECT_URI,
        scope="user-top-read user-read-recently-played user-follow-read playlist-read-collaborative playlist-read-private"
    )
    
    test_db.add(token)
    test_db.commit()
    test_db.refresh(token)
    
    return SpotifyService(test_db)


@pytest.fixture
def trait_extractor(test_db: Session, spotify_service: SpotifyService) -> TraitExtractor:
    """Create a trait extractor instance for testing."""
    return TraitExtractor(test_db, spotify_service)


@pytest.mark.asyncio
async def test_extract_music_traits(trait_extractor: TraitExtractor, test_db: Session):
    """Test extraction of music traits."""
    # Extract music traits
    music_traits = await trait_extractor.extract_music_traits("test_user")
    
    # Verify structure
    assert "genres" in music_traits
    assert "artists" in music_traits
    assert "tracks" in music_traits
    
    # Verify genre preferences
    assert isinstance(music_traits["genres"], dict)
    assert len(music_traits["genres"]) > 0
    
    # Verify artists data
    for term in ["short_term", "medium_term", "long_term"]:
        assert term in music_traits["artists"]
        artists = music_traits["artists"][term]
        assert isinstance(artists, list)
        if len(artists) > 0:  # You might not have data for all terms
            artist = artists[0]
            assert all(key in artist for key in ["id", "name", "genres", "popularity"])
    
    # Verify tracks data
    for term in ["short_term", "medium_term", "long_term"]:
        assert term in music_traits["tracks"]
        tracks = music_traits["tracks"][term]
        assert isinstance(tracks, list)
        if len(tracks) > 0:  # You might not have data for all terms
            track = tracks[0]
            assert all(key in track for key in ["id", "name", "artists", "popularity"])


@pytest.mark.asyncio
async def test_extract_social_traits(trait_extractor: TraitExtractor, test_db: Session):
    """Test extraction of social traits."""
    # Extract social traits
    social_traits = await trait_extractor.extract_social_traits("test_user")
    
    # Verify structure and types
    assert isinstance(social_traits["playlist_count"], int)
    assert isinstance(social_traits["collaborative_playlists"], int)
    assert isinstance(social_traits["public_playlists"], int)
    assert isinstance(social_traits["following_count"], int)
    assert isinstance(social_traits["social_score"], float)
    
    # Verify score bounds
    assert 0 <= social_traits["social_score"] <= 1
    
    # Verify count relationships
    assert social_traits["collaborative_playlists"] <= social_traits["playlist_count"]
    assert social_traits["public_playlists"] <= social_traits["playlist_count"]


@pytest.mark.asyncio
async def test_extract_behavior_traits(trait_extractor: TraitExtractor, test_db: Session):
    """Test extraction of behavior traits."""
    # Extract behavior traits
    behavior_traits = await trait_extractor.extract_behavior_traits("test_user")
    
    # Verify structure
    assert "listening_patterns" in behavior_traits
    patterns = behavior_traits["listening_patterns"]
    
    # Verify peak hours
    assert "peak_hours" in patterns
    assert isinstance(patterns["peak_hours"], list)
    if len(patterns["peak_hours"]) > 0:
        assert all(isinstance(hour, int) for hour in patterns["peak_hours"])
        assert all(0 <= hour <= 23 for hour in patterns["peak_hours"])
    
    # Verify loyal artists
    assert "loyal_artists" in patterns
    assert isinstance(patterns["loyal_artists"], list)
    if len(patterns["loyal_artists"]) > 0:
        assert all(isinstance(artist_id, str) for artist_id in patterns["loyal_artists"])


@pytest.mark.asyncio
async def test_extract_all_traits(trait_extractor: TraitExtractor, test_db: Session):
    """Test extraction of all traits together."""
    # Extract all traits
    traits = await trait_extractor.extract_traits("test_user")
    
    # Verify overall structure
    assert "music" in traits
    assert "social" in traits
    assert "behavior" in traits
    assert "timestamp" in traits
    
    # Verify timestamp
    assert isinstance(traits["timestamp"], str)
    timestamp = datetime.fromisoformat(traits["timestamp"].replace('Z', '+00:00'))
    assert isinstance(timestamp, datetime)
    
    # Verify each trait category has data
    assert len(traits["music"]) > 0
    assert len(traits["social"]) > 0
    assert len(traits["behavior"]) > 0
    
    # Store traits in database
    db_traits = Traits(
        user_id="test_user",
        provider="spotify",
        traits=traits,
        timestamp=timestamp
    )
    test_db.add(db_traits)
    test_db.commit()
    
    # Verify traits were stored correctly
    stored_traits = test_db.query(Traits).filter_by(
        user_id="test_user",
        provider="spotify"
    ).first()
    
    assert stored_traits is not None
    assert stored_traits.traits == traits
    assert stored_traits.timestamp == timestamp


@pytest.mark.asyncio
async def test_update_user_traits(trait_extractor: TraitExtractor, test_db: Session):
    """Test the complete trait update process."""
    # Update all traits
    traits = await trait_extractor.update_user_traits("test_user")
    
    # Verify the Traits model instance
    assert isinstance(traits, Traits)
    assert traits.user_id == "test_user"
    assert isinstance(traits.music_traits, dict)
    assert isinstance(traits.social_traits, dict)
    assert isinstance(traits.behavior_traits, dict)
    
    # Verify timestamps
    assert traits.last_updated_at <= datetime.utcnow()
    assert traits.next_update_at > datetime.utcnow()
    
    # Verify persistence
    db_traits = test_db.query(Traits).filter(Traits.user_id == "test_user").first()
    assert db_traits is not None
    assert db_traits.id == traits.id


@pytest.mark.asyncio
async def test_trait_caching(trait_extractor: TraitExtractor, test_db: Session):
    """Test that traits are properly cached and retrieved."""
    # First extraction (should hit the API)
    start_time = datetime.utcnow()
    traits1 = await trait_extractor.extract_music_traits("test_user")
    first_duration = datetime.utcnow() - start_time
    
    # Second extraction (should hit the cache)
    start_time = datetime.utcnow()
    traits2 = await trait_extractor.extract_music_traits("test_user")
    second_duration = datetime.utcnow() - start_time
    
    # Verify cache hit was faster
    assert second_duration < first_duration
    
    # Verify data consistency
    assert traits1 == traits2
    
    # Verify cache keys
    cache_key = trait_extractor.cache.get_trait_cache_key("test_user", "music")
    assert trait_extractor.cache.get_traits("test_user", "music") is not None 