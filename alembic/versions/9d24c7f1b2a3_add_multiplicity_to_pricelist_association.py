"""Add multiplicity to pricelistautopartassociation

Revision ID: 9d24c7f1b2a3
Revises: 7c3a2b1e9d4f
Create Date: 2026-03-13 00:00:01.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '9d24c7f1b2a3'
down_revision: Union[str, None] = '7c3a2b1e9d4f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'pricelistautopartassociation',
        sa.Column(
            'multiplicity',
            sa.Integer(),
            nullable=False,
            server_default='1',
        ),
    )


def downgrade() -> None:
    op.drop_column('pricelistautopartassociation', 'multiplicity')
