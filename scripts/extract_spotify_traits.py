"""Script to extract and store Spotify traits for a user."""
import asyncio
import os
from datetime import datetime
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.services.spotify import SpotifyService
from app.services.trait_extractor import TraitExtractor
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

async def extract_and_store_traits(user_id: str):
    """Extract and store traits for a user."""
    db = SessionLocal()
    try:
        logger.info(f"Starting trait extraction for user {user_id}")
        
        # Initialize services
        spotify_service = SpotifyService(db)
        trait_extractor = TraitExtractor(db, spotify_service)
        
        # Get Spotify client to verify connection
        client = await spotify_service.get_client(user_id)
        if not client:
            logger.error("Failed to get Spotify client")
            return
            
        # Extract and store traits
        traits = await trait_extractor.update_user_traits(user_id)
        
        logger.info(f"Successfully extracted and stored traits for user {user_id}")
        logger.info("Trait summary:")
        logger.info(f"  Music traits: {len(traits.music_traits)} categories")
        logger.info(f"  Social traits: {len(traits.social_traits)} categories")
        logger.info(f"  Behavior traits: {len(traits.behavior_traits)} categories")
        logger.info(f"  Last updated: {traits.last_updated_at}")
        logger.info(f"  Next update: {traits.next_update_at}")
        
    except Exception as e:
        logger.error(f"Error extracting traits: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
    finally:
        db.close()

if __name__ == "__main__":
    user_id = "loganzechella"  # Your Spotify username
    asyncio.run(extract_and_store_traits(user_id)) 