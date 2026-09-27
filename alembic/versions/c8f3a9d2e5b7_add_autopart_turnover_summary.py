"""Add autopart turnover summary table.

Revision ID: c8f3a9d2e5b7
Revises: b5d9f2a4c6e8
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c8f3a9d2e5b7"
down_revision: Union[str, None] = "b5d9f2a4c6e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autopartturnoversummary",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "autopart_id",
            sa.Integer(),
            sa.ForeignKey("autopart.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("sold_qty_30d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sold_qty_90d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("daily_velocity_30d", sa.Float(), nullable=False, server_default="0"),
        sa.Column("daily_velocity_90d", sa.Float(), nullable=False, server_default="0"),
        sa.Column("supplier_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("min_purchase_price", sa.DECIMAL(10, 2), nullable=True),
        sa.Column("avg_purchase_price", sa.DECIMAL(10, 2), nullable=True),
        sa.Column("supplier_qty_trend_30d", sa.Float(), nullable=True),
        sa.Column("turnover_percentile", sa.Float(), nullable=True),
        sa.Column("is_top", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_unique_constraint(
        "uq_autopartturnoversummary_autopart_id",
        "autopartturnoversummary",
        ["autopart_id"],
    )
    op.create_index(
        "ix_autopartturnoversummary_autopart_id",
        "autopartturnoversummary",
        ["autopart_id"],
    )
    op.create_index(
        "ix_autopartturnoversummary_turnover_percentile",
        "autopartturnoversummary",
        ["turnover_percentile"],
    )
    op.create_index(
        "ix_autopartturnoversummary_is_top",
        "autopartturnoversummary",
        ["is_top"],
    )


def downgrade() -> None:
    op.drop_index("ix_autopartturnoversummary_is_top", table_name="autopartturnoversummary")
    op.drop_index(
        "ix_autopartturnoversummary_turnover_percentile",
        table_name="autopartturnoversummary",
    )
    op.drop_index("ix_autopartturnoversummary_autopart_id", table_name="autopartturnoversummary")
    op.drop_constraint(
        "uq_autopartturnoversummary_autopart_id",
        "autopartturnoversummary",
        type_="unique",
    )
    op.drop_table("autopartturnoversummary")
