"""Script to verify stored LinkedIn traits in the database."""

import asyncio
import logging
from datetime import datetime
from app.database import get_db
from app.services.trait_extractor import TraitExtractor
from app.models.traits import Traits

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_traits(user_id: str):
    """Query and display stored LinkedIn traits for a user."""
    try:
        # Get database session
        db = next(get_db())
        
        # Query traits directly from database
        traits = db.query(Traits).filter_by(user_id=user_id).first()
        if not traits or not traits.professional_traits:
            logger.info(f"No traits found for user {user_id}")
            return
            
        # Display all professional traits
        logger.info("Professional traits in database:")
        logger.info(f"Raw traits: {traits.professional_traits}")
        
        # Display LinkedIn traits
        linkedin_traits = traits.professional_traits.get("linkedin", {})
        if linkedin_traits:
            logger.info("\nFound LinkedIn traits:")
            logger.info(f"Name: {linkedin_traits.get('name')}")
            logger.info(f"Email: {linkedin_traits.get('email')}")
            logger.info(f"Picture URL: {linkedin_traits.get('picture_url')}")
            logger.info(f"Country: {linkedin_traits.get('locale')}")
            logger.info(f"Language: {linkedin_traits.get('language')}")
            logger.info(f"Source: {linkedin_traits.get('source')}")
            
            # Parse and format timestamps
            last_update = linkedin_traits.get('last_updated')
            if last_update:
                logger.info(f"Last Updated: {last_update}")
                
            next_update = linkedin_traits.get('next_update')
            if next_update:
                logger.info(f"Next Update: {next_update}")
                
            last_linkedin_update = traits.professional_traits.get("last_linkedin_update")
            if last_linkedin_update:
                logger.info(f"Last LinkedIn Update: {last_linkedin_update}")
        else:
            logger.info("No LinkedIn traits found in professional_traits")
            
    except Exception as e:
        logger.error(f"Error verifying traits: {str(e)}")
        raise
    finally:
        db.close()

async def main():
    """Main function."""
    # Use test user ID
    user_id = "test_user"
    await verify_traits(user_id)

if __name__ == "__main__":
    asyncio.run(main()) 