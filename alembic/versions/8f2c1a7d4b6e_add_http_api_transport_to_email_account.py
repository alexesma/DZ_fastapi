"""Add HTTP API transport to email account

Revision ID: 8f2c1a7d4b6e
Revises: 6a1d3b7f9e22
Create Date: 2026-03-20 20:05:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = '8f2c1a7d4b6e'
down_revision = '6a1d3b7f9e22'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'emailaccount',
        sa.Column('transport', sa.String(length=32), nullable=True),
    )
    op.add_column(
        'emailaccount',
        sa.Column('http_api_provider', sa.String(length=64), nullable=True),
    )
    op.add_column(
        'emailaccount',
        sa.Column('http_api_url', sa.String(length=512), nullable=True),
    )
    op.add_column(
        'emailaccount',
        sa.Column('http_api_key', sa.String(length=2048), nullable=True),
    )
    op.add_column(
        'emailaccount',
        sa.Column(
            'http_api_timeout',
            sa.Integer(),
            nullable=False,
            server_default='20',
        ),
    )
    op.execute(
        "UPDATE emailaccount SET transport = 'smtp' "
        "WHERE transport IS NULL"
    )
    op.alter_column('emailaccount', 'transport', nullable=False)
    op.alter_column(
        'emailaccount',
        'http_api_timeout',
        server_default=None,
    )


def downgrade():
    op.drop_column('emailaccount', 'http_api_timeout')
    op.drop_column('emailaccount', 'http_api_key')
    op.drop_column('emailaccount', 'http_api_url')
    op.drop_column('emailaccount', 'http_api_provider')
    op.drop_column('emailaccount', 'transport')
