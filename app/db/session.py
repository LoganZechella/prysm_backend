"""Database session configuration."""
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from typing import Generator

# Default PostgreSQL connection URL
DEFAULT_DATABASE_URL = "postgresql://logan@localhost:5432/prysm"

# Get database URL from environment or use default
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

# Ensure URL starts with postgresql:// for SQLAlchemy
if SQLALCHEMY_DATABASE_URL.startswith("postgres://"):
    SQLALCHEMY_DATABASE_URL = SQLALCHEMY_DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Create sync engine with reasonable defaults for PostgreSQL
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,  # Enable connection pool pre-ping
    pool_size=10,        # Default pool size
    max_overflow=20,     # Allow up to 20 connections over pool_size
    pool_timeout=30,     # Timeout after 30 seconds
    pool_recycle=1800    # Recycle connections after 30 minutes
)

# Create sync session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Convert to async URL
ASYNC_DATABASE_URL = SQLALCHEMY_DATABASE_URL.replace(
    "postgresql://",
    "postgresql+asyncpg://",
    1
)

# Create async engine with same connection settings
async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

# Create base class for declarative models
Base = declarative_base()

def get_db() -> Generator:
    """Get database session for synchronous operations."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_async_db() -> AsyncSession:
    """Get async database session for asynchronous operations."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close() 