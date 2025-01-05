"""Add oauth tokens table

Revision ID: 33d69def99ed
Revises: 
Create Date: 2025-01-04 18:48:33.757951

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '33d69def99ed'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('oauth_tokens',
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('service', sa.String(), nullable=False),
    sa.Column('access_token', sa.String(), nullable=False),
    sa.Column('refresh_token', sa.String(), nullable=True),
    sa.Column('token_type', sa.String(), nullable=True),
    sa.Column('scope', sa.String(), nullable=True),
    sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('extra', sa.JSON(), server_default='{}', nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('user_id', 'service')
    )
    op.create_index(op.f('ix_oauth_tokens_user_id'), 'oauth_tokens', ['user_id'], unique=False)
    op.create_table('user_preferences',
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('preferred_categories', sa.JSON(), server_default='[]', nullable=True),
    sa.Column('excluded_categories', sa.JSON(), server_default='[]', nullable=True),
    sa.Column('min_price', sa.Float(), server_default='0.0', nullable=True),
    sa.Column('max_price', sa.Float(), server_default='1000.0', nullable=True),
    sa.Column('preferred_location', sa.JSON(), server_default='{}', nullable=True),
    sa.Column('preferred_days', sa.JSON(), server_default='[]', nullable=True),
    sa.Column('preferred_times', sa.JSON(), server_default='[]', nullable=True),
    sa.Column('min_rating', sa.Float(), server_default='0.5', nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    sa.PrimaryKeyConstraint('user_id')
    )
    op.create_index(op.f('ix_user_preferences_user_id'), 'user_preferences', ['user_id'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_user_preferences_user_id'), table_name='user_preferences')
    op.drop_table('user_preferences')
    op.drop_index(op.f('ix_oauth_tokens_user_id'), table_name='oauth_tokens')
    op.drop_table('oauth_tokens')
    # ### end Alembic commands ###