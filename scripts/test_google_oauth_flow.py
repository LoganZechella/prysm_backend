import asyncio
import sys
import logging

from app.services.google.google_service import GooglePeopleService

# Importing the AsyncSessionLocal maker from app.db.session
from app.db.session import AsyncSessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main(user_id: str):
    async with AsyncSessionLocal() as session:
        google_service = GooglePeopleService(session)
        logger.info(f"Fetching Google professional traits for user: {user_id}")
        traits = await google_service.get_professional_traits(user_id)
        if traits:
            logger.info("Retrieved Google Traits:\n%s", traits)
        else:
            logger.error("No traits retrieved. Please ensure a valid OAuth token exists for this user.")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python test_google_oauth_flow.py <user_id>")
        sys.exit(1)
    user_id = sys.argv[1]
    asyncio.run(main(user_id)) 