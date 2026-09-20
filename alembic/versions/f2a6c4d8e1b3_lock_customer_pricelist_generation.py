"""Lock customer pricelist generation per config.

Revision ID: f2a6c4d8e1b3
Revises: e9f5a3b2c8d1
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f2a6c4d8e1b3"
down_revision: Union[str, None] = "e9f5a3b2c8d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistconfig",
        sa.Column("generation_lock_token", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "customerpricelistconfig",
        sa.Column("generation_locked_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistconfig", "generation_locked_at")
    op.drop_column("customerpricelistconfig", "generation_lock_token")
