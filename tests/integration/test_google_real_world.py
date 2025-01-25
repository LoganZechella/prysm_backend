"""Real-world integration tests for Google People API integration."""
import os
import pytest
from datetime import datetime
import asyncio
from httpx import AsyncClient

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.google import GooglePeopleService
from scripts.create_google_token import create_token

@pytest.mark.real
@pytest.mark.integration
class TestGoogleRealWorld:
    """Real-world integration tests using actual Google API."""

    @classmethod
    def setup_class(cls):
        """Set up test class - verify environment."""
        required_env_vars = [
            'GOOGLE_CLIENT_ID',
            'GOOGLE_CLIENT_SECRET',
            'GOOGLE_REDIRECT_URI',
            'GOOGLE_PROJECT_ID'
        ]
        
        missing_vars = [var for var in required_env_vars if not getattr(settings, var)]
        if missing_vars:
            pytest.skip(f"Missing required environment variables: {', '.join(missing_vars)}")
            
        cls.db = SessionLocal()
        
    @classmethod
    def teardown_class(cls):
        """Clean up after tests."""
        cls.db.close()

    async def test_1_create_token(self):
        """Test creating a real Google OAuth token."""
        # Note: This will open a browser for OAuth consent
        print("\nThis test will open your browser for Google OAuth consent.")
        print("Please log in with the Google account you want to test with.")
        input("Press Enter to continue...")
        
        token = create_token(self.db, "test_user")
        assert token is not None
        assert token.access_token is not None
        assert token.refresh_token is not None
        print(f"\nSuccessfully created token for test user")
        
        # Store token for other tests
        self.__class__.test_token = token
        self.__class__.test_user_id = token.user_id

    @pytest.mark.asyncio
    async def test_2_get_profile_data(self):
        """Test getting real profile data from Google."""
        if not hasattr(self.__class__, 'test_token'):
            pytest.skip("No valid token available - run test_create_token first")
            
        google_service = GooglePeopleService(self.db)
        profile_data = await google_service.get_profile_data(self.test_user_id)
        
        assert profile_data is not None
        print("\nRetrieved profile data:")
        print(f"Names: {profile_data.get('names', [])}")
        print(f"Email Addresses: {profile_data.get('emailAddresses', [])}")
        print(f"Organizations: {profile_data.get('organizations', [])}")
        
        # Store profile data for other tests
        self.__class__.profile_data = profile_data

    @pytest.mark.asyncio
    async def test_3_extract_traits(self):
        """Test extracting real professional traits."""
        if not hasattr(self.__class__, 'test_token'):
            pytest.skip("No valid token available - run test_create_token first")
            
        google_service = GooglePeopleService(self.db)
        traits = await google_service.extract_professional_traits(self.test_user_id)
        
        assert traits is not None
        print("\nExtracted professional traits:")
        if traits.get('organizations'):
            print("\nOrganizations:")
            for org in traits['organizations']:
                print(f"- {org.get('name')} ({org.get('title')})")
                
        if traits.get('skills'):
            print("\nSkills:")
            for skill in traits['skills']:
                print(f"- {skill}")
                
        if traits.get('interests'):
            print("\nInterests:")
            for interest in traits['interests']:
                print(f"- {interest}")

    @pytest.mark.asyncio
    async def test_4_token_refresh(self):
        """Test token refresh with real expired token."""
        if not hasattr(self.__class__, 'test_token'):
            pytest.skip("No valid token available - run test_create_token first")
            
        # Force token expiration
        token = self.test_token
        token.expires_at = datetime.utcnow()
        self.db.commit()
        
        # Try to get profile data (should trigger refresh)
        google_service = GooglePeopleService(self.db)
        profile_data = await google_service.get_profile_data(self.test_user_id)
        
        assert profile_data is not None
        print("\nSuccessfully refreshed token and retrieved profile data")
        
        # Verify token was refreshed
        refreshed_token = (
            self.db.query(OAuthToken)
            .filter_by(user_id=self.test_user_id, provider="google")
            .first()
        )
        assert refreshed_token.expires_at > datetime.utcnow()

def main():
    """Run the tests manually."""
    pytest.main([__file__, "-v", "-s"])

if __name__ == "__main__":
    main() 