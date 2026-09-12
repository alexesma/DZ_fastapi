"""add Parts-Soft order snapshots

Revision ID: a1c4e7b9d2f6
Revises: f8d3b7c1a5e9
Create Date: 2026-09-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a1c4e7b9d2f6"
down_revision: Union[str, None] = "f8d3b7c1a5e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "partssoftordersnapshot",
        sa.Column("external_order_id", sa.String(length=128), nullable=False),
        sa.Column("order_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_customer_id", sa.BigInteger(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_partssoftordersnapshot_external_order_id", "partssoftordersnapshot", ["external_order_id"], unique=True)
    op.create_index("ix_partssoftordersnapshot_order_created_at", "partssoftordersnapshot", ["order_created_at"], unique=False)
    op.create_index("ix_partssoftordersnapshot_external_customer_id", "partssoftordersnapshot", ["external_customer_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_partssoftordersnapshot_external_customer_id", table_name="partssoftordersnapshot")
    op.drop_index("ix_partssoftordersnapshot_order_created_at", table_name="partssoftordersnapshot")
    op.drop_index("ix_partssoftordersnapshot_external_order_id", table_name="partssoftordersnapshot")
    op.drop_table("partssoftordersnapshot")
