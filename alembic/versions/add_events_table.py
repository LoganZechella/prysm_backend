"""add events table

Revision ID: add_events_table
Revises: merge_heads_2
Create Date: 2024-01-25 14:45:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON

# revision identifiers, used by Alembic.
revision = 'add_events_table'
down_revision = 'merge_heads_2'
branch_labels = None
depends_on = None

def upgrade():
    """Add events table."""
    op.create_table(
        'events',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('title', sa.String(), nullable=False),
        sa.Column('description', sa.String(), nullable=True),
        sa.Column('category', sa.String(), nullable=True),
        sa.Column('subcategory', sa.String(), nullable=True),
        sa.Column('location', sa.String(), nullable=True),
        sa.Column('venue', sa.String(), nullable=True),
        sa.Column('start_time', sa.DateTime(), nullable=True),
        sa.Column('end_time', sa.DateTime(), nullable=True),
        sa.Column('price', sa.Float(), nullable=True),
        sa.Column('currency', sa.String(), server_default='USD', nullable=True),
        sa.Column('capacity', sa.Integer(), nullable=True),
        sa.Column('current_attendance', sa.Integer(), server_default='0', nullable=True),
        sa.Column('status', sa.String(), server_default='active', nullable=True),
        sa.Column('organizer_id', sa.String(), nullable=True),
        sa.Column('metadata', JSON, server_default='{}', nullable=True),
        sa.Column('feature_vector', JSON, server_default='{}', nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['organizer_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Add indexes
    op.create_index(op.f('ix_events_category'), 'events', ['category'], unique=False)
    op.create_index(op.f('ix_events_location'), 'events', ['location'], unique=False)
    op.create_index(op.f('ix_events_start_time'), 'events', ['start_time'], unique=False)
    op.create_index(op.f('ix_events_status'), 'events', ['status'], unique=False)

def downgrade():
    """Remove events table."""
    op.drop_index(op.f('ix_events_status'), table_name='events')
    op.drop_index(op.f('ix_events_start_time'), table_name='events')
    op.drop_index(op.f('ix_events_location'), table_name='events')
    op.drop_index(op.f('ix_events_category'), table_name='events')
    op.drop_table('events') 