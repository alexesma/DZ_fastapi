"""Add fixed price to customer pricelist publication rules.

Revision ID: c9f2a4b7d631
Revises: c8e1f4a6b920
Create Date: 2026-09-02 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c9f2a4b7d631"
down_revision: Union[str, Sequence[str], None] = "c8e1f4a6b920"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistpublicationrule",
        sa.Column("fixed_price", sa.Numeric(precision=10, scale=2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistpublicationrule", "fixed_price")
