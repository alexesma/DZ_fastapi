"""add masking to customer pricelist source

Revision ID: e3b4c5d6e7f8
Revises: e2a3b4c5d6e7
Create Date: 2026-06-24 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e3b4c5d6e7f8"
down_revision: Union[str, Sequence[str], None] = "e2a3b4c5d6e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistsource",
        sa.Column(
            "mask_price_quantity",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistsource", "mask_price_quantity")
