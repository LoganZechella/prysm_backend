"""create spotify_profiles table

Revision ID: spotify_profiles_001
Revises: abcdef12345
Create Date: 2025-02-01
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'spotify_profiles_001'
down_revision = 'abcdef12345'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'spotify_profiles',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('user_id', sa.String, unique=True, index=True, nullable=False),
        sa.Column('profile_data', sa.JSON, nullable=True)
    )


def downgrade():
    op.drop_table('spotify_profiles') 