"""add sent state to autopurchase items

Revision ID: b2d3e4f5a6b7
Revises: a1c2e3f4b5d6
Create Date: 2026-05-30 18:45:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2d3e4f5a6b7"
down_revision: Union[str, Sequence[str], None] = "a1c2e3f4b5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "autopurchaserunitem",
        sa.Column("sent_order_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "autopurchaserunitem",
        sa.Column("sent_customer_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "autopurchaserunitem",
        sa.Column("sent_to_site_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "autopurchaserunitem",
        sa.Column("sent_order_number", sa.String(length=36), nullable=True),
    )
    op.add_column(
        "autopurchaserunitem",
        sa.Column("send_result_snapshot", sa.JSON(), nullable=True),
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_sent_order_id"),
        "autopurchaserunitem",
        ["sent_order_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_sent_customer_id"),
        "autopurchaserunitem",
        ["sent_customer_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_autopurchaserunitem_sent_order_id_order",
        "autopurchaserunitem",
        "order",
        ["sent_order_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_autopurchaserunitem_sent_customer_id_customer",
        "autopurchaserunitem",
        "customer",
        ["sent_customer_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_autopurchaserunitem_sent_customer_id_customer",
        "autopurchaserunitem",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_autopurchaserunitem_sent_order_id_order",
        "autopurchaserunitem",
        type_="foreignkey",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_sent_customer_id"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_sent_order_id"),
        table_name="autopurchaserunitem",
    )
    op.drop_column("autopurchaserunitem", "send_result_snapshot")
    op.drop_column("autopurchaserunitem", "sent_order_number")
    op.drop_column("autopurchaserunitem", "sent_to_site_at")
    op.drop_column("autopurchaserunitem", "sent_customer_id")
    op.drop_column("autopurchaserunitem", "sent_order_id")
