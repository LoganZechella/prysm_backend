import pytest
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from app.collectors.meta import MetaCollector
from app.utils.storage import StorageManager
import httpx

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Testing Meta collector...")
print("=" * 50)

@pytest.mark.asyncio
async def test_meta_collector():
    """Test the Meta collector functionality"""
    collector = MetaCollector()
    
    # Test token verification directly
    token_valid = await collector.verify_token()
    assert token_valid, "Token verification failed"
    logger.info("Successfully verified token")
    
    # Test getting user's pages
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{collector.base_url}/me/accounts",
            params={
                "access_token": collector.access_token,
                "fields": "id,name"
            }
        )
        assert response.status_code == 200, f"Failed to get pages: {response.text}"
        data = response.json()
        logger.info(f"Successfully got pages: {data}")
        
    # If we get here, the token is working
    logger.info("Token is valid and working correctly")

if __name__ == "__main__":
    pytest.main([__file__]) 