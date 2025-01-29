"""Database connection and session management."""

from contextlib import contextmanager
from typing import Generator
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from app.config import get_settings
from .base import Base

logger = logging.getLogger(__name__)

def get_engine():
    """Create SQLAlchemy engine with configuration."""
    settings = get_settings()
    return create_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW
    )

# Create engine and session factory
engine = get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Initialize database schema."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database schema created successfully")
    except SQLAlchemyError as e:
        logger.error(f"Error creating database schema: {str(e)}")
        raise

def get_db() -> Generator[Session, None, None]:
    """Get database session."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

@contextmanager
def db_session():
    """Context manager for database sessions."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {str(e)}")
        raise
    finally:
        session.close()

def get_db_session() -> Session:
    """
    Get a new database session.
    
    Returns:
        SQLAlchemy Session object
    """
    return SessionLocal() 