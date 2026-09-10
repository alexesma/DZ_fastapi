"""Guard customer pricelist source freshness.

Revision ID: c9f1a4b6d8e0
Revises: b8e0f3a5c7d9
Create Date: 2026-09-10 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c9f1a4b6d8e0"
down_revision: Union[str, Sequence[str], None] = "b8e0f3a5c7d9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistconfig",
        sa.Column(
            "max_source_age_business_days",
            sa.Integer(),
            server_default="1",
            nullable=False,
        ),
    )
    op.add_column(
        "customerpricelistconfig",
        sa.Column(
            "block_stale_sources",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
    )
    op.add_column(
        "customerpricelistconfig",
        sa.Column("last_stale_blocked_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistconfig", "last_stale_blocked_at")
    op.drop_column("customerpricelistconfig", "block_stale_sources")
    op.drop_column("customerpricelistconfig", "max_source_age_business_days")
