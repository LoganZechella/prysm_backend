"""
Script to create a LinkedIn OAuth token for testing purposes.
"""

import os
import json
import logging
import asyncio
import webbrowser
from datetime import datetime, timedelta
from urllib.parse import urlencode
import secrets

from dotenv import load_dotenv
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

async def create_token(user_id: str = "test_user"):
    """Create a LinkedIn OAuth token."""
    try:
        client_id = os.getenv("LINKEDIN_CLIENT_ID")
        redirect_uri = os.getenv("LINKEDIN_REDIRECT_URI")
        
        logger.info("Creating token with following configuration:")
        logger.info(f"Client ID: {client_id}")
        logger.info(f"Redirect URI: {redirect_uri}")
        logger.info(f"Using test user ID: {user_id}")

        # Generate state for CSRF protection
        state = secrets.token_urlsafe(32)
        
        # Construct LinkedIn authorization URL with required scopes
        # Using scopes as shown in LinkedIn Developer Console
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "state": state,
            "scope": "openid profile email"  # Use authorized scopes from Developer Console
        }
        
        auth_url = f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(params)}"
        
        print("\nStarting LinkedIn OAuth consent flow...")
        print("Your browser should open automatically.")
        print("If it doesn't, please use this URL:")
        print(auth_url)
        print("\nAfter authorization, you will be redirected to your callback URL.")
        print("The FastAPI endpoint will handle the token exchange and storage.")
        
        # Open the authorization URL in the default browser
        webbrowser.open(auth_url)
        
        # Wait for user input to confirm completion
        input("\nPress Enter after completing the authorization in your browser...")
        
        print("\nOAuth flow completed! You can close this window.")
        
    except Exception as e:
        logger.error(f"Error creating token: {str(e)}")
        raise

async def main():
    """Main function."""
    print("\nUsing default test user ID: 'test_user'")
    print("This ID will be used to store your LinkedIn OAuth token in the database.")
    input("Press Enter to continue or Ctrl+C to cancel...")
    
    await create_token()

if __name__ == "__main__":
    asyncio.run(main()) 