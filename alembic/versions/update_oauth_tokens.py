"""update oauth tokens

Revision ID: update_oauth_tokens
Revises: 50fcb0de098b
Create Date: 2024-01-17 01:45:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'update_oauth_tokens'
down_revision = '50fcb0de098b'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Add new columns to oauth_tokens table
    op.add_column('oauth_tokens', sa.Column('client_id', sa.String(length=255), nullable=True))
    op.add_column('oauth_tokens', sa.Column('client_secret', sa.String(length=255), nullable=True))
    op.add_column('oauth_tokens', sa.Column('redirect_uri', sa.String(length=255), nullable=True))
    op.add_column('oauth_tokens', sa.Column('token_type', sa.String(length=50), nullable=True))
    op.add_column('oauth_tokens', sa.Column('scope', sa.String(length=1000), nullable=True))
    op.add_column('oauth_tokens', sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('oauth_tokens', sa.Column('provider_user_id', sa.String(length=255), nullable=True))
    op.add_column('oauth_tokens', sa.Column('provider_metadata', postgresql.JSONB, nullable=True))
    
    # Update existing rows with default values
    op.execute("""
        UPDATE oauth_tokens 
        SET 
            client_id = current_setting('prysm.spotify_client_id', true),
            client_secret = current_setting('prysm.spotify_client_secret', true),
            redirect_uri = current_setting('prysm.spotify_redirect_uri', true),
            token_type = 'Bearer',
            provider_metadata = '{}'::jsonb
        WHERE provider = 'spotify'
    """)
    
    op.execute("""
        UPDATE oauth_tokens 
        SET 
            client_id = current_setting('prysm.google_client_id', true),
            client_secret = current_setting('prysm.google_client_secret', true),
            redirect_uri = current_setting('prysm.google_redirect_uri', true),
            token_type = 'Bearer',
            provider_metadata = '{}'::jsonb
        WHERE provider = 'google'
    """)
    
    op.execute("""
        UPDATE oauth_tokens 
        SET 
            client_id = current_setting('prysm.linkedin_client_id', true),
            client_secret = current_setting('prysm.linkedin_client_secret', true),
            redirect_uri = current_setting('prysm.linkedin_redirect_uri', true),
            token_type = 'Bearer',
            provider_metadata = '{}'::jsonb
        WHERE provider = 'linkedin'
    """)
    
    # Make columns non-nullable after setting defaults
    op.alter_column('oauth_tokens', 'client_id',
        existing_type=sa.String(length=255),
        nullable=False
    )
    op.alter_column('oauth_tokens', 'client_secret',
        existing_type=sa.String(length=255),
        nullable=False
    )
    op.alter_column('oauth_tokens', 'redirect_uri',
        existing_type=sa.String(length=255),
        nullable=False
    )

def downgrade() -> None:
    # Drop new columns from oauth_tokens table
    op.drop_column('oauth_tokens', 'provider_metadata')
    op.drop_column('oauth_tokens', 'provider_user_id')
    op.drop_column('oauth_tokens', 'last_used_at')
    op.drop_column('oauth_tokens', 'scope')
    op.drop_column('oauth_tokens', 'token_type')
    op.drop_column('oauth_tokens', 'redirect_uri')
    op.drop_column('oauth_tokens', 'client_secret')
    op.drop_column('oauth_tokens', 'client_id') 