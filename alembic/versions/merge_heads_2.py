"""merge heads

Revision ID: merge_heads_2
Revises: add_professional_traits, add_traits_table
Create Date: 2024-01-25 06:35:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'merge_heads_2'
down_revision = ('add_professional_traits', 'add_traits_table')
branch_labels = None
depends_on = None

def upgrade():
    """Merge heads."""
    pass

def downgrade():
    """Downgrade both branches."""
    pass 