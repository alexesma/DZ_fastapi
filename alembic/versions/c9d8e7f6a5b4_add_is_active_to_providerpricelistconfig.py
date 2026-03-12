"""add is_active to providerpricelistconfig

Revision ID: c9d8e7f6a5b4
Revises: b7c8d9e0f1a2
Create Date: 2026-03-12 22:10:00.000000
"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = 'c9d8e7f6a5b4'
down_revision = 'b7c8d9e0f1a2'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'providerpricelistconfig',
        sa.Column(
            'is_active',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )
    op.alter_column(
        'providerpricelistconfig',
        'is_active',
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column('providerpricelistconfig', 'is_active')
