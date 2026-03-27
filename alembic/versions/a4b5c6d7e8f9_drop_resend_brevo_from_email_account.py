"""Drop Resend/Brevo fields from email account

Revision ID: a4b5c6d7e8f9
Revises: 8f2c1a7d4b6e
Create Date: 2026-03-21 21:40:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'a4b5c6d7e8f9'
down_revision = '8f2c1a7d4b6e'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        "UPDATE emailaccount SET transport = 'smtp' "
        "WHERE transport = 'http_api'"
    )
    op.drop_column('emailaccount', 'http_api_timeout')
    op.drop_column('emailaccount', 'http_api_key')
    op.drop_column('emailaccount', 'http_api_url')
    op.drop_column('emailaccount', 'http_api_provider')


def downgrade():
    op.add_column(
        'emailaccount',
        sa.Column(
            'http_api_provider', sa.String(length=64), nullable=True
        ),
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
    op.alter_column('emailaccount', 'http_api_timeout', server_default=None)
