"""merge synthetic data heads

Revision ID: merge_synthetic_data_heads
Revises: 7b03196a3c2c, add_synthetic_data_flag, spotify_profiles_001
Create Date: 2024-02-20 10:30:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic
revision = 'merge_synthetic_data_heads'
down_revision = None
branch_labels = None
depends_on = None

# Multiple heads being merged
depends_on = ['7b03196a3c2c', 'add_synthetic_data_flag', 'spotify_profiles_001']

def upgrade() -> None:
    pass

def downgrade() -> None:
    pass 