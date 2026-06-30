"""add customer order forwarding settings

Revision ID: f4a5b6c7d8e9
Revises: e3b4c5d6e7f8
Create Date: 2026-06-30 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f4a5b6c7d8e9"
down_revision: Union[str, Sequence[str], None] = "e3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorderconfig",
        sa.Column(
            "forward_customer_order_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "customerorderconfig",
        sa.Column(
            "forward_customer_order_email",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "customerorderconfig",
        sa.Column(
            "forward_customer_order_email_account_id",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_customerorderconfig_forward_email_account_id",
        "customerorderconfig",
        "emailaccount",
        ["forward_customer_order_email_account_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_customerorderconfig_forward_email_account_id",
        "customerorderconfig",
        type_="foreignkey",
    )
    op.drop_column(
        "customerorderconfig",
        "forward_customer_order_email_account_id",
    )
    op.drop_column("customerorderconfig", "forward_customer_order_email")
    op.drop_column("customerorderconfig", "forward_customer_order_enabled")
