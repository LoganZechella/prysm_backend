from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from fastapi import Request
import jwt
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

class Token(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    expires_in: int
    token_type: str
    scope: Optional[str] = None
    created_at: str = datetime.utcnow().isoformat()

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        """Override dict method to ensure datetime is serialized"""
        d = super().dict(*args, **kwargs)
        if isinstance(d.get("created_at"), datetime):
            d["created_at"] = d["created_at"].isoformat()
        return d

    @classmethod
    def from_token_data(cls, token_data: Dict[str, Any]) -> "Token":
        """Create a Token instance from OAuth response data"""
        return cls(
            access_token=token_data["access_token"],
            refresh_token=token_data.get("refresh_token"),
            expires_in=token_data["expires_in"],
            token_type=token_data["token_type"],
            scope=token_data.get("scope"),
            created_at=datetime.utcnow().isoformat()
        )

    def is_expired(self) -> bool:
        """Check if the token is expired"""
        created = datetime.fromisoformat(self.created_at)
        expiry_time = created + timedelta(seconds=self.expires_in)
        return datetime.utcnow() > expiry_time

class TokenManager:
    def __init__(self, request: Request):
        self.request = request
        self.session = request.session
    
    def store_token(self, service: str, token_data: Dict[str, Any]) -> None:
        """Store OAuth token in session"""
        try:
            logger.info(f"Storing token for {service}")
            token = Token.from_token_data(token_data)
            self.session[f"{service}_token"] = token.dict()
            logger.info(f"Successfully stored token for {service}")
        except Exception as e:
            logger.error(f"Error storing token for {service}: {str(e)}")
            raise
    
    def get_token(self, service: str) -> Optional[Token]:
        """Retrieve token from session"""
        try:
            token_data = self.session.get(f"{service}_token")
            if not token_data:
                return None
            return Token(**token_data)
        except Exception as e:
            logger.error(f"Error retrieving token for {service}: {str(e)}")
            return None
    
    def is_token_valid(self, service: str) -> bool:
        """Check if token is valid and not expired"""
        try:
            token = self.get_token(service)
            if not token:
                return False
            return not token.is_expired()
        except Exception as e:
            logger.error(f"Error checking token validity for {service}: {str(e)}")
            return False
    
    def clear_token(self, service: str) -> None:
        """Remove token from session"""
        try:
            if f"{service}_token" in self.session:
                del self.session[f"{service}_token"]
                logger.info(f"Cleared token for {service}")
        except Exception as e:
            logger.error(f"Error clearing token for {service}: {str(e)}")
            raise
    
    def clear_all_tokens(self) -> None:
        """Remove all tokens from session"""
        try:
            for service in ["spotify", "google", "linkedin"]:
                self.clear_token(service)
            logger.info("Cleared all tokens")
        except Exception as e:
            logger.error(f"Error clearing all tokens: {str(e)}")
            raise