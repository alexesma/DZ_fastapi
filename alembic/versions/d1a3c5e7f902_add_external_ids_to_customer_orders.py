"""Add external identities for idempotent customer order imports.

Revision ID: d1a3c5e7f902
Revises: c9f2a4b7d631
Create Date: 2026-09-06 10:05:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d1a3c5e7f902"
down_revision: Union[str, Sequence[str], None] = "c9f2a4b7d631"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerorder",
        sa.Column("external_source", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "customerorder",
        sa.Column("external_order_id", sa.String(length=128), nullable=True),
    )
    op.create_index(
        op.f("ix_customerorder_external_source"),
        "customerorder",
        ["external_source"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_customerorder_external_source_order_id",
        "customerorder",
        ["external_source", "external_order_id"],
    )
    op.add_column(
        "customerorderitem",
        sa.Column("external_order_item_id", sa.String(length=128), nullable=True),
    )
    op.create_unique_constraint(
        "uq_customerorderitem_order_external_item_id",
        "customerorderitem",
        ["order_id", "external_order_item_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_customerorderitem_order_external_item_id",
        "customerorderitem",
        type_="unique",
    )
    op.drop_column("customerorderitem", "external_order_item_id")
    op.drop_constraint(
        "uq_customerorder_external_source_order_id",
        "customerorder",
        type_="unique",
    )
    op.drop_index(op.f("ix_customerorder_external_source"), table_name="customerorder")
    op.drop_column("customerorder", "external_order_id")
    op.drop_column("customerorder", "external_source")
