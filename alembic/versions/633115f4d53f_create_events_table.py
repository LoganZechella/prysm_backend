"""create_events_table

Revision ID: 633115f4d53f
Revises: e9a4954592c9
Create Date: 2025-01-25 18:42:21.624288

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func


# revision identifiers, used by Alembic.
revision: str = '633115f4d53f'
down_revision: Union[str, None] = 'e9a4954592c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('events', 'created_at',
        existing_type=sa.DateTime(),
        server_default=sa.text('now()'),
        nullable=False
    )
    op.alter_column('events', 'updated_at',
        existing_type=sa.DateTime(),
        server_default=sa.text('now()'),
        nullable=False
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('events', 'created_at',
        existing_type=sa.DateTime(),
        server_default=None,
        nullable=True
    )
    op.alter_column('events', 'updated_at',
        existing_type=sa.DateTime(),
        server_default=None,
        nullable=True
    )
    # ### end Alembic commands ###
