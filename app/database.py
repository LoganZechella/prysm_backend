from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData

# Default to development database URL if not set
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://logan@localhost:5432/prysm"
)

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Configure naming convention for constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

# Create metadata with naming convention and allow table redefinition
metadata = MetaData(naming_convention=convention)
metadata.reflect(bind=engine)

# Configure metadata to allow table redefinition by default
for table in metadata.tables.values():
    table.extend_existing = True

class Base(DeclarativeBase):
    """Base class for all database models."""
    metadata = metadata
    __abstract__ = True
    
    def __repr__(self):
        return f"<{self.__class__.__name__} {self.id}>"

def get_db():
    """Get database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 