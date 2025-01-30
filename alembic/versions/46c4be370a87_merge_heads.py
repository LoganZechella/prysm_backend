"""Merge heads

Revision ID: 46c4be370a87
Revises: 0c945d15d264
Create Date: 2025-01-29 22:24:35.858975

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '46c4be370a87'
down_revision: Union[str, None] = '0c945d15d264'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
