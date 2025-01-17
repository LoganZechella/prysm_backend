import asyncpg
from typing import AsyncGenerator, Dict, Any
import pytest
from ..config.test_config import TestConfig

class TestDatabase:
    def __init__(self):
        self.config = TestConfig()
        self._pool = None

    async def init_pool(self):
        """Initialize database connection pool"""
        if not self._pool:
            self._pool = await asyncpg.create_pool(**self.config.get_database_config())

    async def cleanup(self):
        """Clean up database connections"""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def reset_test_data(self):
        """Reset database to known test state"""
        if not self._pool:
            await self.init_pool()
            
        async with self._pool.acquire() as conn:
            await conn.execute('BEGIN')
            try:
                await self._clear_existing_data(conn)
                await self._load_test_data(conn)
                await conn.execute('COMMIT')
            except Exception:
                await conn.execute('ROLLBACK')
                raise

    async def _clear_existing_data(self, conn):
        """Clear all test data from database"""
        tables = [
            'events',
            'users',
            'preferences',
            'recommendations',
            'error_logs'
        ]
        for table in tables:
            await conn.execute(f'TRUNCATE TABLE {table} CASCADE')

    async def _load_test_data(self, conn):
        """Load fresh test data into database"""
        if 'events' in self.config.test_data:
            await self._load_events(conn, self.config.test_data['events'])
        if 'users' in self.config.test_data:
            await self._load_users(conn, self.config.test_data['users'])
        if 'preferences' in self.config.test_data:
            await self._load_preferences(conn, self.config.test_data['preferences'])

    async def _load_events(self, conn, events: Dict[str, Any]):
        """Load test events into database"""
        for event in events:
            await conn.execute('''
                INSERT INTO events (id, title, description, start_date, location)
                VALUES ($1, $2, $3, $4, $5)
            ''', event['id'], event['title'], event['description'],
                event['start_date'], event['location'])

    async def _load_users(self, conn, users: Dict[str, Any]):
        """Load test users into database"""
        for user in users:
            await conn.execute('''
                INSERT INTO users (id, email, name)
                VALUES ($1, $2, $3)
            ''', user['id'], user['email'], user['name'])

    async def _load_preferences(self, conn, preferences: Dict[str, Any]):
        """Load test preferences into database"""
        for pref in preferences:
            await conn.execute('''
                INSERT INTO preferences (user_id, categories, price_range)
                VALUES ($1, $2, $3)
            ''', pref['user_id'], pref['categories'], pref['price_range'])

@pytest.fixture
async def test_db():
    """Fixture providing test database"""
    db = TestDatabase()
    await db.init_pool()
    await db.reset_test_data()
    yield db
    await db.cleanup() 