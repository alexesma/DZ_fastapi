"""add email account oauth fields

Revision ID: d7e4f5a6b7c8
Revises: c5d1a2b3c4d5
Create Date: 2026-02-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd7e4f5a6b7c8'
down_revision = 'c5d1a2b3c4d5'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'emailaccount',
        sa.Column('oauth_provider', sa.String(length=32), nullable=True),
    )
    op.add_column(
        'emailaccount',
        sa.Column(
            'oauth_refresh_token', sa.String(length=2048), nullable=True
        ),
    )
    op.add_column(
        'emailaccount',
        sa.Column(
            'oauth_connected_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        'emailaccount',
        sa.Column(
            'oauth_updated_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column('emailaccount', 'oauth_updated_at')
    op.drop_column('emailaccount', 'oauth_connected_at')
    op.drop_column('emailaccount', 'oauth_refresh_token')
    op.drop_column('emailaccount', 'oauth_provider')
