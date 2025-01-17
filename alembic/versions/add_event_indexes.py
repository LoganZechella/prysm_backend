"""Add event indexes

Revision ID: add_event_indexes
Revises: 8a59475911f7
Create Date: 2024-03-10 10:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'add_event_indexes'
down_revision: Union[str, None] = '8a59475911f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Add indexes for common query patterns
    
    # Index for start_time queries (used in date filtering)
    op.create_index(
        'ix_events_start_time',
        'events',
        ['start_time'],
        postgresql_using='btree'
    )
    
    # Index for categories array (used in category filtering)
    op.create_index(
        'ix_events_categories',
        'events',
        ['categories'],
        postgresql_using='gin'
    )
    
    # Composite index for location-based queries
    op.execute(
        'CREATE INDEX ix_events_location ON events USING gist (location)'
    )
    
    # Index for price range queries
    op.execute(
        'CREATE INDEX ix_events_price_info ON events USING gin (price_info jsonb_path_ops)'
    )
    
    # Index for popularity score (used in trending queries)
    op.create_index(
        'ix_events_popularity_score',
        'events',
        ['popularity_score'],
        postgresql_using='btree'
    )
    
    # Index for source and source_id (used in deduplication)
    op.create_index(
        'ix_events_source_source_id',
        'events',
        ['source', 'source_id'],
        unique=True,
        postgresql_using='btree'
    )

def downgrade() -> None:
    # Remove indexes in reverse order
    op.drop_index('ix_events_source_source_id')
    op.drop_index('ix_events_popularity_score')
    op.execute('DROP INDEX ix_events_price_info')
    op.execute('DROP INDEX ix_events_location')
    op.drop_index('ix_events_categories')
    op.drop_index('ix_events_start_time') 