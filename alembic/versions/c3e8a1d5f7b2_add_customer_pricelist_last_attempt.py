"""Track customer pricelist delivery attempts.

Revision ID: c3e8a1d5f7b2
Revises: a9d4c7e2f6b1
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c3e8a1d5f7b2"
down_revision: Union[str, None] = "a9d4c7e2f6b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistconfig",
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("customerpricelistconfig", "last_attempt_at")
