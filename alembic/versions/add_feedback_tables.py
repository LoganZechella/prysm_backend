"""add feedback tables

Revision ID: add_feedback_tables
Revises: merge_heads_3
Create Date: 2025-01-29 16:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'add_feedback_tables'
down_revision: Union[str, None] = 'merge_heads_3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create event_scores table
    op.create_table(
        'event_scores',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('category_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('price_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('location_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('time_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('engagement_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('final_score', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('score_context', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_event_scores_id'), 'event_scores', ['id'], unique=False)
    
    # Create user_feedback table
    op.create_table(
        'user_feedback',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('rating', sa.Float(), nullable=True),
        sa.Column('liked', sa.Boolean(), nullable=True),
        sa.Column('saved', sa.Boolean(), nullable=True),
        sa.Column('comment', sa.String(), nullable=True),
        sa.Column('feedback_context', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_user_feedback_id'), 'user_feedback', ['id'], unique=False)
    
    # Create implicit_feedback table
    op.create_table(
        'implicit_feedback',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('event_id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('view_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('view_duration', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('click_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('share_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('feedback_context', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['event_id'], ['events.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_implicit_feedback_id'), 'implicit_feedback', ['id'], unique=False)

def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('implicit_feedback')
    op.drop_table('user_feedback')
    op.drop_table('event_scores') 