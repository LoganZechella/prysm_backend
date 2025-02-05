"""Add synthetic data flag to feedback tables

Revision ID: add_synthetic_data_flag
Revises: abcdef12345
Create Date: 2024-02-20 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic
revision = 'add_synthetic_data_flag'
down_revision = 'abcdef12345'  # Update this to your last migration
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Add is_synthetic column to implicit_feedback table
    op.add_column('implicit_feedback',
        sa.Column('is_synthetic', sa.Boolean(), nullable=False, server_default='false')
    )
    
    # Add is_synthetic column to user_feedback table
    op.add_column('user_feedback',
        sa.Column('is_synthetic', sa.Boolean(), nullable=False, server_default='false')
    )

def downgrade() -> None:
    # Remove is_synthetic column from implicit_feedback table
    op.drop_column('implicit_feedback', 'is_synthetic')
    
    # Remove is_synthetic column from user_feedback table
    op.drop_column('user_feedback', 'is_synthetic') 