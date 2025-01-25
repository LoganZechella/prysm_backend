"""Service for extracting user traits from Spotify data."""
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session

from app.models.traits import Traits
from app.services.spotify import SpotifyService
from app.core.cache import CacheManager, TRAIT_CACHE_TTL


class TraitExtractor:
    """Service for extracting and managing user traits."""

    def __init__(self, db: Session, spotify_service: SpotifyService):
        """Initialize the trait extractor service."""
        self.db = db
        self.spotify_service = spotify_service
        self.cache = CacheManager()

    async def extract_music_traits(self, user_id: str) -> Dict[str, Any]:
        """Extract music-related traits from Spotify."""
        # Check cache first
        cached_traits = self.cache.get_traits(user_id, "music")
        if cached_traits:
            return cached_traits

        # Get Spotify data
        top_tracks_short = await self.spotify_service.get_top_tracks(limit=50, time_range="short_term")
        top_tracks_medium = await self.spotify_service.get_top_tracks(limit=50, time_range="medium_term")
        top_tracks_long = await self.spotify_service.get_top_tracks(limit=50, time_range="long_term")
        
        top_artists_short = await self.spotify_service.get_top_artists(limit=50, time_range="short_term")
        top_artists_medium = await self.spotify_service.get_top_artists(limit=50, time_range="medium_term")
        top_artists_long = await self.spotify_service.get_top_artists(limit=50, time_range="long_term")

        # Process and structure the data
        music_traits = {
            "genres": self._extract_genre_preferences(
                (top_artists_short.get('items', []) if top_artists_short else []) +
                (top_artists_medium.get('items', []) if top_artists_medium else []) +
                (top_artists_long.get('items', []) if top_artists_long else [])
            ),
            "artists": {
                "short_term": self._process_artists(top_artists_short.get('items', []) if top_artists_short else []),
                "medium_term": self._process_artists(top_artists_medium.get('items', []) if top_artists_medium else []),
                "long_term": self._process_artists(top_artists_long.get('items', []) if top_artists_long else [])
            },
            "tracks": {
                "short_term": self._process_tracks(top_tracks_short.get('items', []) if top_tracks_short else []),
                "medium_term": self._process_tracks(top_tracks_medium.get('items', []) if top_tracks_medium else []),
                "long_term": self._process_tracks(top_tracks_long.get('items', []) if top_tracks_long else [])
            }
        }

        # Cache the results
        self.cache.set_traits(user_id, "music", music_traits)
        
        return music_traits

    async def extract_social_traits(self, user_id: str) -> Dict[str, Any]:
        """Extract social-related traits from Spotify."""
        # Check cache first
        cached_traits = self.cache.get_traits(user_id, "social")
        if cached_traits:
            return cached_traits

        # Get Spotify data
        playlists = await self.spotify_service.get_user_playlists()
        following = await self.spotify_service.get_followed_artists()
        
        # Process and structure the data
        social_traits = {
            "playlist_count": playlists.get('total', 0) if playlists else 0,
            "collaborative_playlists": sum(1 for p in playlists.get('items', []) if p.get("collaborative", False)) if playlists else 0,
            "public_playlists": sum(1 for p in playlists.get('items', []) if p.get("public", False)) if playlists else 0,
            "following_count": following.get('artists', {}).get('total', 0) if following else 0,
            "social_score": self._calculate_social_score(
                playlists.get('items', []) if playlists else [],
                following.get('artists', {}).get('items', []) if following and 'artists' in following else []
            )
        }

        # Cache the results
        self.cache.set_traits(user_id, "social", social_traits)
        
        return social_traits

    async def extract_behavior_traits(self, user_id: str) -> Dict[str, Any]:
        """Extract behavior-related traits from Spotify."""
        # Check cache first
        cached_traits = self.cache.get_traits(user_id, "behavior")
        if cached_traits:
            return cached_traits

        # Get Spotify data
        recently_played = await self.spotify_service.get_recently_played(limit=50)
        
        # Process and structure the data
        behavior_traits = {
            "listening_times": self._analyze_listening_times(recently_played.get('items', []) if recently_played else []),
            "discovery_ratio": self._calculate_discovery_ratio(recently_played.get('items', []) if recently_played else []),
            "engagement_level": self._calculate_engagement_level(recently_played.get('items', []) if recently_played else [])
        }

        # Cache the results
        self.cache.set_traits(user_id, "behavior", behavior_traits)
        
        return behavior_traits

    async def update_user_traits(self, user_id: str) -> Traits:
        """Update all traits for a user."""
        # Extract traits from Spotify
        spotify_traits = await self.spotify_service.extract_user_traits(user_id)

        # Get existing traits or create new
        traits = (
            self.db.query(Traits)
            .filter(Traits.user_id == user_id)
            .first()
        )

        if not traits:
            traits = Traits(user_id=user_id)
            self.db.add(traits)

        # Update traits
        traits.music_traits = spotify_traits.get('music', {})
        traits.social_traits = spotify_traits.get('social', {}).get('playlist_behavior', {})
        traits.behavior_traits = spotify_traits.get('behavior', {}).get('listening_patterns', {})
        traits.last_updated_at = datetime.utcnow()
        traits.next_update_at = datetime.utcnow() + TRAIT_CACHE_TTL

        # Save to database
        self.db.commit()
        self.db.refresh(traits)

        return traits

    def _extract_genre_preferences(self, artists: List[Dict[str, Any]]) -> Dict[str, float]:
        """Extract and score genre preferences from artists."""
        genre_scores = {}
        total_weight = 0
        
        for i, artist in enumerate(artists):
            weight = 1 / (i + 1)  # Higher weight for higher-ranked artists
            total_weight += weight
            
            for genre in artist.get("genres", []):
                genre_scores[genre] = genre_scores.get(genre, 0) + weight

        # Normalize scores
        if total_weight > 0:
            return {
                genre: score / total_weight
                for genre, score in genre_scores.items()
            }
        return {}

    def _process_artists(self, artists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process artist data for storage."""
        return [
            {
                "id": artist["id"],
                "name": artist["name"],
                "genres": artist.get("genres", []),
                "popularity": artist.get("popularity", 0)
            }
            for artist in artists
        ]

    def _process_tracks(self, tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process track data for storage."""
        return [
            {
                "id": track["id"],
                "name": track["name"],
                "artists": [
                    {"id": artist["id"], "name": artist["name"]}
                    for artist in track.get("artists", [])
                ],
                "popularity": track.get("popularity", 0)
            }
            for track in tracks
        ]

    def _calculate_social_score(
        self,
        playlists: List[Dict[str, Any]],
        following: List[Dict[str, Any]]
    ) -> float:
        """Calculate a social engagement score."""
        playlist_score = len(playlists) * 0.3
        collab_score = sum(1 for p in playlists if p.get("collaborative", False)) * 0.5
        public_score = sum(1 for p in playlists if p.get("public", False)) * 0.2
        following_score = len(following) * 0.2
        
        max_score = 100
        raw_score = playlist_score + collab_score + public_score + following_score
        
        return min(raw_score, max_score) / max_score

    def _analyze_listening_times(self, recently_played: List[Dict[str, Any]]) -> Dict[str, int]:
        """Analyze when the user typically listens to music."""
        time_slots = {
            "morning": 0,   # 6-12
            "afternoon": 0, # 12-18
            "evening": 0,   # 18-24
            "night": 0      # 0-6
        }
        
        for track in recently_played:
            played_at = datetime.fromisoformat(track["played_at"].replace("Z", "+00:00"))
            hour = played_at.hour
            
            if 6 <= hour < 12:
                time_slots["morning"] += 1
            elif 12 <= hour < 18:
                time_slots["afternoon"] += 1
            elif 18 <= hour < 24:
                time_slots["evening"] += 1
            else:
                time_slots["night"] += 1
                
        return time_slots

    def _calculate_discovery_ratio(self, recently_played: List[Dict[str, Any]]) -> float:
        """Calculate the ratio of new vs. familiar music."""
        unique_tracks = len(set(track["track"]["id"] for track in recently_played))
        return unique_tracks / len(recently_played) if recently_played else 0

    def _calculate_engagement_level(self, recently_played: List[Dict[str, Any]]) -> float:
        """Calculate user engagement level based on listening patterns."""
        if not recently_played:
            return 0.0
            
        # Calculate time gaps between plays
        timestamps = [
            datetime.fromisoformat(track["played_at"].replace("Z", "+00:00"))
            for track in recently_played
        ]
        timestamps.sort(reverse=True)
        
        gaps = []
        for i in range(len(timestamps) - 1):
            gap = timestamps[i] - timestamps[i + 1]
            gaps.append(gap.total_seconds() / 3600)  # Convert to hours
            
        if not gaps:
            return 0.0
            
        # Calculate average gap
        avg_gap = sum(gaps) / len(gaps)
        
        # Score based on average gap (lower gap = higher engagement)
        max_expected_gap = 24  # hours
        engagement_score = 1 - (min(avg_gap, max_expected_gap) / max_expected_gap)
        
        return engagement_score 