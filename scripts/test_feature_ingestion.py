"""Test script for feature ingestion pipeline."""
import logging
from app.recommendation.features.ingestion import FeatureIngestion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_sample_user_data():
    """Generate sample user data for testing."""
    return {
        'id': 'test_user_1',
        'music_preferences': {
            'genres': ['rock', 'jazz', 'electronic'],
            'favorite_artists': ['Artist1', 'Artist2', 'Artist3']
        },
        'music_history': [
            {'artist': 'Artist1', 'track': 'Track1', 'timestamp': '2024-01-27T10:00:00Z'},
            {'artist': 'Artist2', 'track': 'Track2', 'timestamp': '2024-01-27T11:00:00Z'}
        ],
        'social_data': {
            'connections': 150,
            'engagement_rate': 0.75,
            'activity_score': 0.8
        },
        'professional_data': {
            'skills': ['python', 'machine learning', 'data science'],
            'industries': ['technology', 'artificial intelligence'],
            'interests': ['software development', 'ai research']
        },
        'activity_history': [
            {'type': 'event_attendance', 'timestamp': '2024-01-20T19:00:00Z'},
            {'type': 'profile_update', 'timestamp': '2024-01-25T15:30:00Z'}
        ]
    }

def get_sample_event_data():
    """Generate sample event data for testing."""
    return {
        'id': 'test_event_1',
        'title': 'Tech Meetup: AI and Machine Learning',
        'description': 'Join us for an evening of discussions about the latest in AI and ML.',
        'categories': ['technology', 'networking', 'education'],
        'price': 25.0,
        'capacity': 100,
        'start_time': '2024-02-15T18:00:00Z',
        'end_time': '2024-02-15T21:00:00Z',
        'organizer': {
            'id': 'org_1',
            'rating': 4.8,
            'past_events': 15
        },
        'engagement': {
            'registered': 75,
            'interested': 150,
            'social_shares': 25
        }
    }

def main():
    """Run feature ingestion test."""
    try:
        logger.info("Initializing feature ingestion test...")
        ingestion = FeatureIngestion()
        
        # Test user feature ingestion
        logger.info("Testing user feature ingestion...")
        user_data = get_sample_user_data()
        user_success = ingestion.ingest_user_features(user_data)
        logger.info(f"User feature ingestion {'successful' if user_success else 'failed'}")
        
        # Test event feature ingestion
        logger.info("Testing event feature ingestion...")
        event_data = get_sample_event_data()
        event_success = ingestion.ingest_event_features(event_data)
        logger.info(f"Event feature ingestion {'successful' if event_success else 'failed'}")
        
        # Test batch ingestion
        logger.info("Testing batch ingestion...")
        batch_user_results = ingestion.batch_ingest_user_features([user_data, user_data])
        batch_event_results = ingestion.batch_ingest_event_features([event_data, event_data])
        
        logger.info(f"Batch user ingestion results: {batch_user_results}")
        logger.info(f"Batch event ingestion results: {batch_event_results}")
        
        logger.info("Feature ingestion test complete!")
        
    except Exception as e:
        logger.error(f"Error during feature ingestion test: {str(e)}")
        raise

if __name__ == "__main__":
    main() 