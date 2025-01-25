"""Add professional traits column.

Revision ID: add_professional_traits
Revises: merge_heads
Create Date: 2024-01-25 06:34:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = 'add_professional_traits'
down_revision = 'merge_heads'  # Parent revision is merge_heads
branch_labels = None
depends_on = None

def upgrade():
    """Add professional_traits column to traits table."""
    op.add_column(
        'traits',
        sa.Column(
            'professional_traits',
            JSONB,
            nullable=False,
            server_default=text("'{}'")
        )
    )

def downgrade():
    """Remove professional_traits column from traits table."""
    op.drop_column('traits', 'professional_traits') 