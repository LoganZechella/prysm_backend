"""Script to fix database connection."""
import os
from dotenv import load_dotenv
import sys

def fix_db_connection():
    """Fix database connection by updating environment variables."""
    # Load current environment variables
    load_dotenv()
    
    # Set the correct database URLs
    base_url = "postgresql://logan@localhost:5432/prysm"
    async_url = base_url.replace('postgresql://', 'postgresql+asyncpg://')
    
    os.environ["DATABASE_URL"] = base_url
    os.environ["POSTGRESQL_CONNECTION_URI"] = base_url
    os.environ["ASYNC_DATABASE_URL"] = async_url
    
    print("Database connection fixed. The following environment variables have been set:")
    print(f"DATABASE_URL: {os.environ.get('DATABASE_URL')}")
    print(f"POSTGRESQL_CONNECTION_URI: {os.environ.get('POSTGRESQL_CONNECTION_URI')}")
    print(f"ASYNC_DATABASE_URL: {os.environ.get('ASYNC_DATABASE_URL')}")

if __name__ == "__main__":
    fix_db_connection() 