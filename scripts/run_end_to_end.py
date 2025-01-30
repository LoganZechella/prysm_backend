"""Script to run the entire recommendation flow end-to-end."""
import asyncio
import logging
import argparse
import os
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.session import AsyncSessionLocal
from app.services.trait_extractor import TraitExtractor
from app.services.event_collection import EventCollectionService
from app.services.recommendation_engine import RecommendationEngine
from app.services.spotify import SpotifyService
from app.services.google.google_service import GooglePeopleService
from app.services.linkedin.linkedin_service import LinkedInService
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

async def run_end_to_end(user_id: str, skip_events: bool = False):
    """Run the entire recommendation flow for a user."""
    try:
        async with AsyncSessionLocal() as db:
            logger.info(f"Starting end-to-end flow for user {user_id}")
            
            # Step 1: Initialize services
            logger.info("\nInitializing services...")
            spotify_service = SpotifyService(db)
            google_service = GooglePeopleService(db)
            linkedin_service = LinkedInService(db)
            
            # Step 2: Extract user traits
            logger.info("\nExtracting user traits...")
            trait_extractor = TraitExtractor(
                db=db,
                spotify_service=spotify_service,
                google_service=google_service,
                linkedin_service=linkedin_service
            )
            
            traits = await trait_extractor.update_user_traits(user_id)
            if not traits:
                logger.error("Failed to extract user traits")
                return
                
            # Explicitly refresh traits from database
            await db.refresh(traits)
            
            logger.info("\nExtracted traits:")
            logger.info(f"Music traits: {len(traits.music_traits or {})} items")
            logger.info(f"Social traits: {len(traits.social_traits or {})} items")
            logger.info(f"Professional traits: {len(traits.professional_traits or {})} items")
            logger.info(f"Last updated: {traits.last_updated_at}")
            
            # Step 3: Collect events (if not skipped)
            if not skip_events:
                logger.info("\nCollecting events...")
                # Get Scrapfly API key from environment
                api_key = os.getenv("SCRAPFLY_TEST_API_KEY")  # Use test key for development
                if not api_key:
                    logger.error("No Scrapfly API key found in environment")
                    return
                    
                event_service = EventCollectionService(scrapfly_api_key=api_key)
                
                # Get user's location from traits
                location = None
                if traits.professional_traits:
                    for source in ["linkedin", "google"]:
                        if source_traits := traits.professional_traits.get(source):
                            if locations := source_traits.get("locations"):
                                location = locations[0]
                                break
                
                if not location:
                    logger.warning("No location found in user traits, using default")
                    location = {
                        "city": "Louisville",
                        "state": "KY",
                        "country": "US",
                        "latitude": 38.2527,
                        "longitude": -85.7585
                    }
                
                logger.info(f"\nUsing location: {location}")
                
                # Collect events for next 30 days
                date_range = {
                    "start": datetime.utcnow(),
                    "end": datetime.utcnow() + timedelta(days=30)
                }
                
                events_count = await event_service.collect_events(
                    locations=[location],
                    date_range=date_range
                )
                
                logger.info(f"\nCollected {events_count} events")
            else:
                logger.info("Skipping event collection")
            
            # Step 4: Generate recommendations
            logger.info("\nGenerating recommendations...")
            recommendation_engine = RecommendationEngine(db)
            
            # Extract interests from traits
            interests = []
            if traits.professional_traits:
                for source in ["linkedin", "google"]:
                    if source_traits := traits.professional_traits.get(source):
                        if source_interests := source_traits.get("interests"):
                            interests.extend(source_interests)
            
            recommendations = await recommendation_engine.get_recommendations(
                user_id=user_id,
                user_location=location if not skip_events else None,
                preferences={
                    "categories": interests,
                    "max_distance": 50,  # km
                    "max_price": None  # No price limit
                },
                max_results=20
            )
            
            # Log recommendations for review
            logger.info("\nTop recommendations:")
            for i, event in enumerate(recommendations, 1):
                logger.info(f"\nRecommendation {i}:")
                logger.info(f"Title: {event['title']}")
                logger.info(f"Score: {event['relevance_score']:.2f}")
                logger.info(f"Date: {event['start_datetime']}")
                logger.info(f"Location: {event['venue_city']}, {event['venue_state']}")
                logger.info(f"Categories: {event['categories']}")
                logger.info(f"URL: {event['url']}")
            
            await spotify_service.close()
            
            return recommendations
            
    except Exception as e:
        logger.error(f"Error in end-to-end flow: {str(e)}")
        if hasattr(e, '__traceback__'):
            import traceback
            logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run end-to-end recommendation flow")
    parser.add_argument("user_id", help="User ID to generate recommendations for")
    parser.add_argument("--skip-events", action="store_true", help="Skip event collection")
    args = parser.parse_args()
    
    asyncio.run(run_end_to_end(args.user_id, args.skip_events)) 