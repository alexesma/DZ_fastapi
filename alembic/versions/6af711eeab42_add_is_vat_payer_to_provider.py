"""Add is_vat_payer to provider

Revision ID: 6af711eeab42
Revises: c6d7e8f9a0b2
Create Date: 2026-04-15 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6af711eeab42'
down_revision: Union[str, None] = 'c6d7e8f9a0b2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'provider',
        sa.Column(
            'is_vat_payer',
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column('provider', 'is_vat_payer')
