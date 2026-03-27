"""Add Resend API fields to email account

Revision ID: d1e2f3a4b5c6
Revises: a4b5c6d7e8f9
Create Date: 2026-03-27 12:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'd1e2f3a4b5c6'
down_revision = 'a4b5c6d7e8f9'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'emailaccount',
        sa.Column('resend_api_key', sa.String(length=2048), nullable=True),
    )
    op.add_column(
        'emailaccount',
        sa.Column(
            'resend_timeout',
            sa.Integer(),
            nullable=False,
            server_default='20',
        ),
    )
    op.add_column(
        'emailaccount',
        sa.Column(
            'resend_last_received_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.alter_column('emailaccount', 'resend_timeout', server_default=None)


def downgrade():
    op.drop_column('emailaccount', 'resend_last_received_at')
    op.drop_column('emailaccount', 'resend_timeout')
    op.drop_column('emailaccount', 'resend_api_key')
