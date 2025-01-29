from typing import Optional, cast, Union
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
import jwt
from datetime import datetime, timedelta
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not SECRET_KEY:
    raise ValueError("JWT_SECRET_KEY environment variable is not set")

# Convert to bytes for PyJWT
SECRET_KEY_BYTES = SECRET_KEY.encode()

ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

logger.debug(f"Using JWT configuration: ALGORITHM={ALGORITHM}, EXPIRE_MINUTES={ACCESS_TOKEN_EXPIRE_MINUTES}")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_current_user(request: Request) -> str:
    """
    Get the current authenticated user from the JWT token in the request.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        User ID from token
        
    Raises:
        HTTPException: If token is invalid or expired
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Extract token from Authorization header
        auth_header = request.headers.get("Authorization")
        logger.debug(f"Auth header: {auth_header}")
        
        if not auth_header or not auth_header.startswith("Bearer "):
            logger.debug("No Bearer token found in Authorization header")
            raise credentials_exception
            
        token = auth_header.split(" ")[1]
        logger.debug(f"Token: {token}")
        
        try:
            # Verify token
            logger.debug("Attempting to decode token...")
            payload = jwt.decode(token, SECRET_KEY_BYTES, algorithms=[ALGORITHM])
            logger.debug(f"Token payload: {payload}")
            
            user_id = cast(str, payload.get("sub"))
            if user_id is None:
                logger.debug("No user_id found in token payload")
                raise credentials_exception
                
            logger.debug(f"Successfully validated token for user: {user_id}")
            return user_id
            
        except jwt.ExpiredSignatureError:
            logger.debug("Token has expired")
            # Token has expired, but we can still get the user_id for refresh
            payload = jwt.decode(token, SECRET_KEY_BYTES, algorithms=[ALGORITHM], options={"verify_exp": False})
            user_id = cast(str, payload.get("sub"))
            if user_id is None:
                raise credentials_exception
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except jwt.PyJWTError as e:
        logger.debug(f"JWT Error: {str(e)}")
        raise credentials_exception
    except Exception as e:
        logger.debug(f"Unexpected error in get_current_user: {str(e)}")
        raise credentials_exception

def create_access_token(user_id: str) -> str:
    """
    Create a new JWT access token.
    
    Args:
        user_id: User ID to encode in token
        
    Returns:
        JWT access token
    """
    logger.debug(f"Creating access token for user: {user_id}")
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {
        "sub": user_id,  # Simplified token structure
        "exp": expire
    }
    logger.debug(f"Token payload to encode: {to_encode}")
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY_BYTES, algorithm=ALGORITHM)
    # Handle different return types from jwt.encode
    if isinstance(encoded_jwt, (bytes, bytearray)):
        return encoded_jwt.decode('utf-8')
    return str(encoded_jwt)  # Convert any other type to string 