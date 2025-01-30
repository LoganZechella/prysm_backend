"""Script to verify OAuth tokens for all services."""
import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import AsyncSessionLocal
from app.services.spotify import SpotifyService
from app.services.google.google_service import GooglePeopleService
from app.services.linkedin.linkedin_service import LinkedInService
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

async def verify_tokens(user_id: str):
    """Verify OAuth tokens for all services."""
    async with AsyncSessionLocal() as db:
        try:
            logger.info(f"Verifying tokens for user {user_id}")
            
            # Initialize services
            spotify_service = SpotifyService(db)
            google_service = GooglePeopleService(db)
            linkedin_service = LinkedInService(db)
            
            # Test Spotify connection
            logger.info("\nTesting Spotify connection...")
            spotify_client = await spotify_service.get_client(user_id)
            if spotify_client:
                logger.info("✓ Spotify connection successful")
            else:
                logger.error("✗ Spotify connection failed")
            
            # Test Google connection
            logger.info("\nTesting Google connection...")
            google_client = await google_service.get_client(user_id)
            if google_client:
                logger.info("✓ Google connection successful")
            else:
                logger.error("✗ Google connection failed")
            
            # Test LinkedIn connection
            logger.info("\nTesting LinkedIn connection...")
            linkedin_client = await linkedin_service.get_client(user_id)
            if linkedin_client:
                logger.info("✓ LinkedIn connection successful")
            else:
                logger.error("✗ LinkedIn connection failed")
            
        except Exception as e:
            logger.error(f"Error verifying tokens: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Verify OAuth tokens")
    parser.add_argument("user_id", help="User ID to verify tokens for")
    args = parser.parse_args()
    
    asyncio.run(verify_tokens(args.user_id)) 