"""add pricelist stale override

Revision ID: e7c2a9f4b1d3
Revises: c9f1a4b6d8e0
Create Date: 2026-09-10
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e7c2a9f4b1d3"
down_revision: Union[str, None] = "c9f1a4b6d8e0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column("stale_override_until", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("providerpricelistconfig", "stale_override_until")
