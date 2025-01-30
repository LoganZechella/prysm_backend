"""Merge all heads

Revision ID: 25ff89e1a7e5
Revises: 10727570b098, 46c4be370a87, add_technical_level
Create Date: 2025-01-29 22:24:51.020911

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '25ff89e1a7e5'
down_revision: Union[str, None] = ('10727570b098', '46c4be370a87', 'add_technical_level')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
