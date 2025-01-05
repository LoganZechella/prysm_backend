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

async def verify_session(request: Request) -> SessionContainer:
    """Verify and return the current session.
    
    This function is used as a FastAPI dependency to protect routes.
    It verifies the session token and returns the session if valid.
    
    Args:
        request: The FastAPI request object
        
    Returns:
        SessionContainer: The verified session container
        
    Raises:
        HTTPException: If the session is invalid or expired
    """
    try:
        session = await get_session(request, True)
        if not session:
            logger.warning("No active session found")
            raise HTTPException(
                status_code=401,
                detail="No active session found"
            )
        return session
    except Exception as e:
        logger.error(f"Error verifying session: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired session"
        )

async def create_session(request: Request, user_id: str) -> SessionContainer:
    """Create a new session for the user.
    
    Args:
        request: The FastAPI request object
        user_id: The user ID to associate with the session
        
    Returns:
        SessionContainer: The created session container
        
    Raises:
        HTTPException: If session creation fails
    """
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
        logger.error(f"Error creating session: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to create session"
        )