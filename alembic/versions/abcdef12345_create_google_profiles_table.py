"""
create google_profiles table

Revision ID: abcdef12345
Revises: 30e957cc4ab7
Create Date: 2023-10-XX
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'abcdef12345'
down_revision = '30e957cc4ab7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'google_profiles',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('user_id', sa.String, unique=True, index=True, nullable=False),
        sa.Column('profile_data', sa.JSON, nullable=True)
    )


def downgrade():
    op.drop_table('google_profiles') 