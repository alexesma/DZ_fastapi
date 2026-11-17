"""Add multiple supplier order recipient emails.

Revision ID: f7c9a2e4b681
Revises: e6a8c0d2f591
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f7c9a2e4b681"
down_revision: Union[str, Sequence[str], None] = "e6a8c0d2f591"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "providerpricelistconfig",
        sa.Column(
            "order_emails",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.execute(
        """
        UPDATE providerpricelistconfig
        SET order_emails = json_build_array(trim(order_email_to))
        WHERE order_email_to IS NOT NULL AND trim(order_email_to) <> ''
        """
    )
    op.alter_column(
        "providerpricelistconfig",
        "order_emails",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("providerpricelistconfig", "order_emails")
