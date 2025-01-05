"""initial_schema

Revision ID: 50fcb0de098b
Revises: 
Create Date: 2024-01-05 07:34:56.789012

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '50fcb0de098b'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create user_preferences table
    op.create_table(
        'user_preferences',
        sa.Column('user_id', sa.String(length=255), nullable=False),
        sa.Column('preferred_categories', postgresql.JSONB, nullable=True),
        sa.Column('excluded_categories', postgresql.JSONB, nullable=True),
        sa.Column('min_price', sa.Float, nullable=True),
        sa.Column('max_price', sa.Float, nullable=True),
        sa.Column('preferred_location', postgresql.JSONB, nullable=True),
        sa.Column('preferred_days', postgresql.JSONB, nullable=True),
        sa.Column('preferred_times', postgresql.JSONB, nullable=True),
        sa.Column('min_rating', sa.Float, nullable=True),
        sa.Column('accessibility_requirements', postgresql.JSONB, nullable=True, server_default='[]'),
        sa.Column('indoor_outdoor_preference', sa.String(length=50), nullable=True),
        sa.Column('age_restriction_preference', sa.String(length=50), nullable=True),
        sa.Column('created_at', sa.DateTime, nullable=True),
        sa.Column('updated_at', sa.DateTime, nullable=True),
        sa.PrimaryKeyConstraint('user_id')
    )

    # Create oauth_tokens table
    op.create_table(
        'oauth_tokens',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('user_id', sa.String(length=255), nullable=False),
        sa.Column('provider', sa.String(length=50), nullable=False),
        sa.Column('access_token', sa.String(length=2000), nullable=False),
        sa.Column('refresh_token', sa.String(length=2000), nullable=True),
        sa.Column('expires_at', sa.DateTime, nullable=True),
        sa.Column('created_at', sa.DateTime, nullable=True),
        sa.Column('updated_at', sa.DateTime, nullable=True)
    )


def downgrade() -> None:
    op.drop_table('oauth_tokens')
    op.drop_table('user_preferences')
