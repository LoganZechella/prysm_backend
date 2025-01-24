"""merge heads

Revision ID: merge_heads
Revises: add_event_indexes, update_oauth_tokens
Create Date: 2024-01-17 01:50:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'merge_heads'
down_revision = ('add_event_indexes', 'update_oauth_tokens')
branch_labels = None
depends_on = None

def upgrade() -> None:
    pass

def downgrade() -> None:
    pass 