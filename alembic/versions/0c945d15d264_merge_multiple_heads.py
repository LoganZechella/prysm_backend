"""merge multiple heads

Revision ID: 0c945d15d264
Revises: add_feedback_tables
Create Date: 2025-01-29 19:47:13.886364

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c945d15d264'
down_revision: Union[str, None] = 'add_feedback_tables'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
