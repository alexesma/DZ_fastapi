"""add collapse_duplicates_by_min_price to customer pricelist config

Revision ID: 6c4d2e1f9a8b
Revises: 2b7c4a1d9e0f, 5a7d9c1e2b3f
Create Date: 2026-04-23 20:40:00.000000
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '6c4d2e1f9a8b'
down_revision: Union[str, Sequence[str], None] = (
    '2b7c4a1d9e0f',
    '5a7d9c1e2b3f',
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'customerpricelistconfig',
        sa.Column(
            'collapse_duplicates_by_min_price',
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )


def downgrade() -> None:
    op.drop_column(
        'customerpricelistconfig',
        'collapse_duplicates_by_min_price',
    )
