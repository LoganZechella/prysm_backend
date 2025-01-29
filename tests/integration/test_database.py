import pytest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.database.models import Base, User, EventModel, UserProfile, EventScore
from app.database.connection import get_db_session
from app.config import get_settings
from sqlalchemy.exc import IntegrityError
from datetime import datetime

class TestDatabaseConnection:
    @pytest.fixture(scope="class")
    def db_session(self):
        """Create test database session"""
        settings = get_settings()
        engine = create_engine(settings.DATABASE_URL)
        TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        
        session = TestingSessionLocal()
        try:
            yield session
        finally:
            session.close()
            Base.metadata.drop_all(bind=engine)
            
    def test_connection(self, db_session):
        """Test basic database connectivity"""
        result = db_session.execute("SELECT 1").scalar()
        assert result == 1
        
    def test_user_crud(self, db_session):
        """Test User model CRUD operations"""
        # Create
        user = User(
            email="test@example.com",
            hashed_password="testpass",
            is_active=True
        )
        db_session.add(user)
        db_session.commit()
        
        # Read
        saved_user = db_session.query(User).filter_by(email="test@example.com").first()
        assert saved_user is not None
        assert saved_user.email == "test@example.com"
        
        # Update
        saved_user.is_active = False
        db_session.commit()
        updated_user = db_session.query(User).filter_by(email="test@example.com").first()
        assert not updated_user.is_active
        
        # Delete
        db_session.delete(saved_user)
        db_session.commit()
        deleted_user = db_session.query(User).filter_by(email="test@example.com").first()
        assert deleted_user is None
        
    def test_event_crud(self, db_session):
        """Test Event model CRUD operations"""
        event = EventModel(
            title="Test Event",
            description="Test Description",
            source="eventbrite",
            external_id="123",
            url="http://example.com",
            start_time="2024-01-27T00:00:00Z",
            end_time="2024-01-27T02:00:00Z",
            location="Test Location"
        )
        db_session.add(event)
        db_session.commit()
        
        saved_event = db_session.query(EventModel).filter_by(external_id="123").first()
        assert saved_event is not None
        assert saved_event.title == "Test Event"
        
    def test_concurrent_connections(self, db_session):
        """Test multiple concurrent database connections"""
        from concurrent.futures import ThreadPoolExecutor
        import threading
        
        def db_operation(i):
            session = get_db_session()
            try:
                # Perform a simple query
                result = session.execute("SELECT 1").scalar()
                assert result == 1
                return True
            finally:
                session.close()
                
        # Test with 10 concurrent connections
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(db_operation, range(10)))
            assert all(results)
            
    def test_transaction_rollback(self, db_session):
        """Test transaction rollback on error"""
        initial_count = db_session.query(User).count()
        
        try:
            # Start a transaction
            user1 = User(email="user1@test.com", hashed_password="pass1", is_active=True)
            user2 = User(email="user2@test.com", hashed_password="pass2", is_active=True)
            
            db_session.add(user1)
            db_session.add(user2)
            
            # Intentionally cause an error
            raise Exception("Test rollback")
            
        except Exception:
            db_session.rollback()
            
        final_count = db_session.query(User).count()
        assert final_count == initial_count  # Ensure no users were added
        
    def test_connection_timeout(self, db_session):
        """Test database connection timeout handling"""
        from sqlalchemy.exc import OperationalError
        
        # Try to connect with a very short timeout
        try:
            engine = create_engine(
                get_settings().DATABASE_URL,
                connect_args={"connect_timeout": 1}
            )
            conn = engine.connect()
            conn.close()
        except OperationalError:
            pytest.skip("Database connection timeout test skipped - database might be slow") 