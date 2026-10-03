"""Add order email subject/recipient to provider price-list config.

Revision ID: f1b7d3e9a5c2
Revises: e6a8c0d2f4b7
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f1b7d3e9a5c2"
down_revision: Union[str, None] = "e6a8c0d2f4b7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column("order_email_subject", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "providerpricelistconfig",
        sa.Column("order_email_to", sa.String(length=500), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("providerpricelistconfig", "order_email_to")
    op.drop_column("providerpricelistconfig", "order_email_subject")
