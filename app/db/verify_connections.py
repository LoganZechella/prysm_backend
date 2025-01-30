import os
import sys
import psycopg2
from sqlalchemy import create_engine, text
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_app_connection():
    """Verify the application's database connection."""
    try:
        db_url = os.getenv("DATABASE_URL", "postgresql://logan@localhost:5432/prysm")
        engine = create_engine(db_url)
        
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.scalar()
            logger.info(f"✅ App Database Connection Successful")
            logger.info(f"PostgreSQL Version: {version}")
            
        return True
    except Exception as e:
        logger.error(f"❌ App Database Connection Failed: {str(e)}")
        return False

def verify_supertokens_connection():
    """Verify SuperTokens database connection."""
    try:
        db_url = os.getenv("POSTGRESQL_CONNECTION_URI", "postgresql://logan@localhost:5432/prysm")
        
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        
        logger.info(f"✅ SuperTokens Database Connection Successful")
        logger.info(f"PostgreSQL Version: {version}")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ SuperTokens Database Connection Failed: {str(e)}")
        return False

def create_database():
    """Create the database if it doesn't exist."""
    try:
        # Connect to default postgres database to create our database
        conn = psycopg2.connect(
            dbname="postgres",
            user="logan",
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Check if database exists
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'prysm';")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute("CREATE DATABASE prysm;")
            logger.info("✅ Database 'prysm' created successfully")
        else:
            logger.info("ℹ️ Database 'prysm' already exists")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Database creation failed: {str(e)}")
        return False

def main():
    """Main verification function."""
    logger.info("Starting database verification...")
    
    # Step 1: Create database
    if not create_database():
        logger.error("Failed to create database. Exiting...")
        sys.exit(1)
    
    # Step 2: Verify app connection
    if not verify_app_connection():
        logger.error("Failed to verify app connection. Exiting...")
        sys.exit(1)
    
    # Step 3: Verify SuperTokens connection
    if not verify_supertokens_connection():
        logger.error("Failed to verify SuperTokens connection. Exiting...")
        sys.exit(1)
    
    logger.info("✅ All database connections verified successfully!")

if __name__ == "__main__":
    main() 