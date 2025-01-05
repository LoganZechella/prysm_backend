from typing import Optional, Dict, Any
from supertokens_python.recipe.session.asyncio import create_new_session, get_session
from supertokens_python.recipe.session import SessionContainer
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer
import logging
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Initialize security scheme
security = HTTPBearer()

async def verify_session(request):
    """Verify and return the session."""
    try:
        session = await get_session(request)
        if not session:
            raise Exception("No session found")
        return session
    except Exception as e:
        logger.error(f"Error verifying session: {str(e)}")
        raise

async def create_session(request, user_id):
    """Create a new session."""
    try:
        session = await create_new_session(
            request,
            user_id=user_id,
            access_token_payload={},
            session_data_in_database={},
            tenant_id="public"
        )
        return session
    except Exception as e:
        logger.error(f"Error creating session: {str(e)}")
        raise