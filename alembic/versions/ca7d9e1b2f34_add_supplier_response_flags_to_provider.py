"""add supplier response flags to provider

Revision ID: ca7d9e1b2f34
Revises: c9d8e7f6a5b4
Create Date: 2026-04-08 13:10:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'ca7d9e1b2f34'
down_revision = 'c9d8e7f6a5b4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'provider',
        sa.Column(
            'supplier_response_allow_shipping_docs',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        'provider',
        sa.Column(
            'supplier_response_allow_response_files',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.add_column(
        'provider',
        sa.Column(
            'supplier_response_allow_text_status',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )

    op.alter_column(
        'provider',
        'supplier_response_allow_shipping_docs',
        server_default=None,
    )
    op.alter_column(
        'provider',
        'supplier_response_allow_response_files',
        server_default=None,
    )
    op.alter_column(
        'provider',
        'supplier_response_allow_text_status',
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column('provider', 'supplier_response_allow_text_status')
    op.drop_column('provider', 'supplier_response_allow_response_files')
    op.drop_column('provider', 'supplier_response_allow_shipping_docs')
