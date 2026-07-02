"""add autopurchase forecast snapshot table (plan vs fact loop)

Revision ID: f6a7b8c9d0e1
Revises: a1b2c3d4e5f7
Create Date: 2026-06-26 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autopurchaseforecastsnapshot",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column(
            "autopart_id",
            sa.Integer(),
            sa.ForeignKey("autopart.id"),
            nullable=True,
        ),
        sa.Column("oem_number", sa.String(255), nullable=False),
        sa.Column("brand_name", sa.String(255), nullable=True),
        sa.Column("forecast_avg_daily", sa.DECIMAL(10, 2), nullable=True),
        sa.Column(
            "recommended_qty",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("proposed_qty", sa.Integer(), nullable=True),
        sa.Column("sent_qty", sa.Integer(), nullable=True),
        sa.Column("purchase_price", sa.DECIMAL(10, 2), nullable=True),
        sa.Column(
            "current_quantity_at_run",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("target_stock", sa.Integer(), nullable=True),
        sa.Column(
            "evaluated_at", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("actual_sold_qty", sa.Integer(), nullable=True),
        sa.Column("actual_avg_daily", sa.DECIMAL(10, 2), nullable=True),
        sa.Column("forecast_error_pct", sa.DECIMAL(10, 2), nullable=True),
        sa.Column("current_quantity_at_eval", sa.Integer(), nullable=True),
        sa.Column("outcome", sa.String(32), nullable=True),
    )
    op.create_index(
        "ix_apfs_created_at",
        "autopurchaseforecastsnapshot",
        ["created_at"],
    )
    op.create_index(
        "ix_apfs_oem_number",
        "autopurchaseforecastsnapshot",
        ["oem_number"],
    )
    op.create_index(
        "ix_apfs_evaluated_at",
        "autopurchaseforecastsnapshot",
        ["evaluated_at"],
    )
    op.create_index(
        "ix_apfs_outcome",
        "autopurchaseforecastsnapshot",
        ["outcome"],
    )
    op.create_index(
        "ix_apfs_run_id",
        "autopurchaseforecastsnapshot",
        ["run_id"],
    )
    op.create_index(
        "ix_apfs_autopart_id",
        "autopurchaseforecastsnapshot",
        ["autopart_id"],
    )


def downgrade() -> None:
    op.drop_table("autopurchaseforecastsnapshot")
