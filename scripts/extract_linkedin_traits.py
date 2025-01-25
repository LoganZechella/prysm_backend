"""Script to test LinkedIn trait extraction."""

import asyncio
import logging
import os
from datetime import datetime
from dotenv import load_dotenv

from app.database import get_db
from app.services.linkedin.linkedin_service import LinkedInService
from app.services.trait_extractor import TraitExtractor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def extract_and_store_traits(user_id: str):
    """Extract and store LinkedIn traits for a user."""
    try:
        # Get database session
        db = next(get_db())
        
        logger.info(f"Starting trait extraction for user {user_id}")
        
        # Initialize services
        linkedin_service = LinkedInService(db)
        trait_extractor = TraitExtractor(db, linkedin_service=linkedin_service)
        
        # Extract traits directly first to verify they can be extracted
        raw_traits = await linkedin_service.extract_professional_traits(user_id)
        if not raw_traits:
            logger.error("Failed to extract LinkedIn traits")
            return
            
        logger.info("Successfully extracted LinkedIn traits:")
        logger.info(f"Name: {raw_traits.get('name')}")
        logger.info(f"Email: {raw_traits.get('email')}")
        logger.info(f"Picture URL: {raw_traits.get('picture_url')}")
        logger.info(f"Country: {raw_traits.get('locale')}")
        logger.info(f"Language: {raw_traits.get('language')}")
        logger.info(f"Last Updated: {raw_traits.get('last_updated')}")
        logger.info(f"Next Update: {raw_traits.get('next_update')}")
        
        # Store traits
        traits = await trait_extractor.update_user_traits(user_id)
        if not traits:
            logger.error("Failed to store traits in database")
            return
            
        logger.info(f"Successfully stored traits in database at: {traits.last_updated_at}")
            
    except Exception as e:
        logger.error(f"Error extracting traits: {str(e)}")
    finally:
        db.close()

async def main():
    """Main function."""
    # Use test user ID
    user_id = "test_user"
    await extract_and_store_traits(user_id)

if __name__ == "__main__":
    asyncio.run(main()) 