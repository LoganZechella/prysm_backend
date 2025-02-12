"""Script to create Google OAuth tokens for testing."""
import os
import json
import argparse
from datetime import datetime, timedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from sqlalchemy.orm import Session
import urllib.parse
import webbrowser

from app.db.session import SessionLocal
from app.models.oauth import OAuthToken
from app.core.config import settings
from app.utils.logging import setup_logger

logger = setup_logger(__name__)

# If modifying scopes, delete the file token.json.
SCOPES = [
    'openid',  # Add openid scope explicitly
    'https://www.googleapis.com/auth/userinfo.profile',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/contacts.readonly',
    'https://www.googleapis.com/auth/user.organization.read',
    'https://www.googleapis.com/auth/user.emails.read',
    'https://www.googleapis.com/auth/user.addresses.read',
    'https://www.googleapis.com/auth/user.birthday.read',
    'https://www.googleapis.com/auth/user.gender.read',
    'https://www.googleapis.com/auth/user.phonenumbers.read'
]

def create_token(db: Session, user_id: str):
    """Create a new Google OAuth token for testing."""
    logger.info("Creating token with following configuration:")
    logger.info(f"Client ID: {settings.GOOGLE_CLIENT_ID}")
    logger.info(f"Project ID: {settings.GOOGLE_PROJECT_ID}")
    logger.info(f"Using user ID: {user_id}")
    
    client_config = {
        "installed": {
            "client_id": settings.GOOGLE_CLIENT_ID,
            "project_id": settings.GOOGLE_PROJECT_ID,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": settings.GOOGLE_CLIENT_SECRET,
            "redirect_uris": [
                "http://localhost:8001/"
            ]
        }
    }

    logger.info("Initializing OAuth flow...")
    # Create flow instance
    flow = InstalledAppFlow.from_client_config(
        client_config,
        SCOPES
    )

    print("\nStarting Google OAuth consent flow...")
    print("Your browser should open automatically.")
    print("If it doesn't, the authorization URL will be displayed here.")
    print("Please complete the authorization in your browser when it opens.")
    
    try:
        creds = flow.run_local_server(
            port=8001,
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent',  # Force consent screen to get refresh token
            success_message="OAuth flow completed! You can close this window."
        )
        print("OAuth flow completed successfully!")
    except Exception as e:
        if "Scope has changed" in str(e):
            # This is an expected warning about scope changes
            logger.info(f"Expected scope change: {str(e)}")
            print("OAuth flow completed with expected scope changes!")
            # Continue with the credentials we got
        else:
            logger.error(f"Error during OAuth flow: {str(e)}")
            raise

    if not creds or not creds.valid:
        logger.error("Failed to obtain valid credentials")
        raise Exception("Failed to obtain valid credentials")

    # Check if token already exists
    existing_token = db.query(OAuthToken).filter_by(
        user_id=user_id,
        provider="google"
    ).first()

    if existing_token:
        # Update existing token
        existing_token.access_token = creds.token
        existing_token.refresh_token = creds.refresh_token
        existing_token.expires_at = datetime.utcnow() + timedelta(seconds=creds.expiry.timestamp() - datetime.utcnow().timestamp())
        existing_token.scope = " ".join(SCOPES)
        token = existing_token
    else:
        # Create new token
        token = OAuthToken(
            user_id=user_id,
            provider="google",
            client_id=settings.GOOGLE_CLIENT_ID,
            client_secret=settings.GOOGLE_CLIENT_SECRET,
            redirect_uri="http://localhost:8001/",
            access_token=creds.token,
            refresh_token=creds.refresh_token,
            token_type="Bearer",
            scope=" ".join(SCOPES),
            expires_at=datetime.utcnow() + timedelta(seconds=creds.expiry.timestamp() - datetime.utcnow().timestamp()),
            provider_metadata={}
        )
        db.add(token)

    # Save to database
    db.commit()
    db.refresh(token)

    logger.info(f"Successfully created token for user {user_id}")
    print(f"\nToken successfully created and stored in database!")
    print(f"User ID: {user_id}")
    print(f"Access Token: {creds.token[:20]}...")
    print(f"Token will expire at: {token.expires_at}")
    if creds.refresh_token:
        print("✓ Refresh token obtained")
    
    return token

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Create Google OAuth token")
    parser.add_argument("user_id", help="User ID to create token for")
    args = parser.parse_args()

    if not all([
        settings.GOOGLE_CLIENT_ID,
        settings.GOOGLE_CLIENT_SECRET,
        settings.GOOGLE_PROJECT_ID
    ]):
        print("Error: Missing Google OAuth configuration in .env file")
        return

    print(f"\nCreating token for user ID: {args.user_id}")
    print("This ID will be used to store your Google OAuth token in the database.")
    input("Press Enter to continue or Ctrl+C to cancel...")

    db = SessionLocal()
    try:
        create_token(db, args.user_id)
    finally:
        db.close()

if __name__ == "__main__":
    main() 