"""Add multiplicity_col to providerpricelistconfig

Revision ID: 7c3a2b1e9d4f
Revises: 6b1e4d9a2f7c
Create Date: 2026-03-13 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '7c3a2b1e9d4f'
down_revision: Union[str, None] = '6b1e4d9a2f7c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'providerpricelistconfig',
        sa.Column('multiplicity_col', sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('providerpricelistconfig', 'multiplicity_col')
