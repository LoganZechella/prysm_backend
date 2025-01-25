"""Add traits table

Revision ID: add_traits_table
Revises: merge_heads
Create Date: 2024-01-24 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID
import uuid

# revision identifiers, used by Alembic.
revision = 'add_traits_table'
down_revision = 'merge_heads'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create traits table
    op.create_table(
        'traits',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('music_traits', JSONB, nullable=False, server_default='{}'),
        sa.Column('social_traits', JSONB, nullable=False, server_default='{}'),
        sa.Column('behavior_traits', JSONB, nullable=False, server_default='{}'),
        sa.Column('trait_metadata', JSONB, nullable=False, server_default='{"version": 1}'),
        sa.Column('last_updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('next_update_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), onupdate=sa.text('now()'), nullable=False),
    )

    # Add indexes
    op.create_index('ix_traits_user_id', 'traits', ['user_id'])
    op.create_index('ix_traits_next_update_at', 'traits', ['next_update_at'])
    op.create_index('ix_traits_last_updated_at', 'traits', ['last_updated_at'])
    
    # Add GIN indexes for JSONB columns for faster querying
    op.execute('CREATE INDEX ix_traits_music_traits ON traits USING GIN (music_traits jsonb_path_ops)')
    op.execute('CREATE INDEX ix_traits_social_traits ON traits USING GIN (social_traits jsonb_path_ops)')
    op.execute('CREATE INDEX ix_traits_behavior_traits ON traits USING GIN (behavior_traits jsonb_path_ops)')


def downgrade() -> None:
    # Drop indexes first
    op.drop_index('ix_traits_behavior_traits', table_name='traits')
    op.drop_index('ix_traits_social_traits', table_name='traits')
    op.drop_index('ix_traits_music_traits', table_name='traits')
    op.drop_index('ix_traits_last_updated_at', table_name='traits')
    op.drop_index('ix_traits_next_update_at', table_name='traits')
    op.drop_index('ix_traits_user_id', table_name='traits')
    
    # Drop the table
    op.drop_table('traits') 