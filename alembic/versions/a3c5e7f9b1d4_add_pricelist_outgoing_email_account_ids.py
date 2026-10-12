"""Add multiple outgoing mailboxes to customer pricelist configs.

Revision ID: a3c5e7f9b1d4
Revises: f1b7d3e9a5c2
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a3c5e7f9b1d4"
down_revision: Union[str, None] = "f1b7d3e9a5c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelistconfig",
        sa.Column(
            "outgoing_email_account_ids",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.execute(
        """
        UPDATE customerpricelistconfig
        SET outgoing_email_account_ids = json_build_array(outgoing_email_account_id)
        WHERE outgoing_email_account_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.drop_column("customerpricelistconfig", "outgoing_email_account_ids")
