"""merge heads

Revision ID: merge_heads_3
Revises: 10727570b098, add_technical_level
Create Date: 2025-01-29 16:24:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'merge_heads_3'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Update the depends_on to include both parent revisions
depends_on = ('10727570b098', 'add_technical_level')

def upgrade() -> None:
    pass

def downgrade() -> None:
    pass 