"""Script to test the recommendation engine."""
import asyncio
import logging
from datetime import datetime
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.recommendation.engine import RecommendationEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_recommendations(user_id: str):
    """Test getting recommendations for a user."""
    logger.info(f"Getting recommendations for user {user_id}")
    
    try:
        # Create database session
        db = SessionLocal()
        
        # Initialize recommendation engine
        engine = RecommendationEngine(db)
        
        # Get recommendations with filters
        filters = {
            "max_price": 100,  # Maximum price of $100
            "min_score": 0.3,  # Minimum relevance score of 0.3
            "sources": ["eventbrite", "meetup"]  # Example sources
        }
        
        recommendations = await engine.get_recommendations(
            user_id=user_id,
            filters=filters
        )
        
        # Log results
        if recommendations:
            logger.info(f"Found {len(recommendations)} recommendations:")
            for rec in recommendations:
                logger.info(
                    f"\nEvent: {rec['title']}\n"
                    f"Description: {rec['description'][:100]}...\n"
                    f"Categories: {rec['categories']}\n"
                    f"Location: {rec['location']}\n"
                    f"Start Time: {rec['start_time']}\n"
                    f"Score: {rec['score']:.2f}\n"
                    f"Explanation: {rec['explanation']}"
                )
        else:
            logger.info("No recommendations found")
            
    except Exception as e:
        logger.error(f"Error testing recommendations: {str(e)}")
        raise
        
    finally:
        db.close()

async def main():
    """Main function."""
    # Use test user ID
    user_id = "test_user"
    await test_recommendations(user_id)

if __name__ == "__main__":
    asyncio.run(main()) 