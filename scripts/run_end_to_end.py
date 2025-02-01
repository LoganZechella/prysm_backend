"""Script to run end-to-end flow for a user."""
import asyncio
import sys
from datetime import datetime
import logging
import click
from typing import Optional

from app.db.session import AsyncSessionLocal
from app.services.trait_extractor import TraitExtractor
from app.services.spotify import SpotifyService

logger = logging.getLogger(__name__)

async def run_end_to_end(user_id: str, skip_events: bool = False):
    """Run end-to-end flow for a user."""
    logger.info(f"Starting end-to-end flow for user {user_id}")
    
    async with AsyncSessionLocal() as db:
        try:
            logger.info("\nInitializing services...")
            spotify_service = SpotifyService(db)
            trait_extractor = TraitExtractor(db, spotify_service=spotify_service)
            
            logger.info("\nExtracting user traits...")
            traits = await trait_extractor.update_user_traits(user_id)
            if not traits:
                logger.error("Failed to extract user traits")
                return
                
            logger.info("\nTrait extraction summary:")
            logger.info(f"  Music traits: {len(traits.music_traits) if traits.music_traits else 0} categories")
            logger.info(f"  Social traits: {len(traits.social_traits) if traits.social_traits else 0} categories")
            logger.info(f"  Behavior traits: {len(traits.behavior_traits) if traits.behavior_traits else 0} categories")
            logger.info(f"  Last updated: {traits.last_updated_at}")
            logger.info(f"  Next update: {traits.next_update_at}")
            
        except Exception as e:
            logger.error(f"Error in end-to-end flow: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")

@click.command()
@click.argument('user_id')
@click.option('--skip-events', is_flag=True, help='Skip event collection')
def main(user_id: str, skip_events: bool):
    """Run end-to-end flow for a user."""
    asyncio.run(run_end_to_end(user_id, skip_events))

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    main() 