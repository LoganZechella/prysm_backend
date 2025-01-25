"""Script to extract and store Google traits for a user."""
import asyncio
import os
from datetime import datetime
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services.google.google_service import GooglePeopleService
from app.services.trait_extractor import TraitExtractor
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

async def extract_and_store_traits(user_id: str):
    """Extract and store traits for a user."""
    db = SessionLocal()
    try:
        logger.info(f"Starting trait extraction for user {user_id}")
        
        # Initialize services
        google_service = GooglePeopleService(db)
        trait_extractor = TraitExtractor(db, google_service=google_service)
        
        # Extract and store traits
        traits = await trait_extractor.update_user_traits(user_id)
        
        # Get professional traits
        professional_traits = await trait_extractor.get_professional_traits(user_id)
        
        logger.info(f"Successfully extracted and stored traits for user {user_id}")
        logger.info("Professional trait summary:")
        logger.info(f"  Organizations: {len(professional_traits.get('organizations', []))} entries")
        logger.info(f"  Occupations: {len(professional_traits.get('occupations', []))} entries")
        logger.info(f"  Locations: {len(professional_traits.get('locations', []))} entries")
        logger.info(f"  Interests: {len(professional_traits.get('interests', []))} entries")
        logger.info(f"  Skills: {len(professional_traits.get('skills', []))} entries")
        logger.info(f"  URLs: {len(professional_traits.get('urls', []))} entries")
        logger.info(f"  Last updated: {traits.last_updated_at}")
        logger.info(f"  Next update: {traits.next_update_at}")
        
    except Exception as e:
        logger.error(f"Error extracting traits: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
    finally:
        db.close()

def main():
    """Main function."""
    user_id = "test_user"  # Use the same test user ID as in create_google_token.py
    asyncio.run(extract_and_store_traits(user_id))

if __name__ == "__main__":
    main() 