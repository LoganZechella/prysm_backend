"""add technical_level column

Revision ID: add_technical_level
Revises: 95f2ab15ca3f
Create Date: 2025-01-29 16:22:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'add_technical_level'
down_revision: Union[str, None] = '95f2ab15ca3f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Add technical_level column with default value
    op.add_column('events', sa.Column('technical_level', sa.Float(), nullable=False, server_default='0.5'))

def downgrade() -> None:
    # Remove technical_level column
    op.drop_column('events', 'technical_level') 