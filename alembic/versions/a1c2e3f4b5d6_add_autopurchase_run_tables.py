"""add autopurchase run tables

Revision ID: a1c2e3f4b5d6
Revises: 9ab0c1d2e3f4
Create Date: 2026-05-30 14:10:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1c2e3f4b5d6"
down_revision: Union[str, Sequence[str], None] = "9ab0c1d2e3f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "autopurchaserun",
        sa.Column("provider_config_id", sa.Integer(), nullable=False),
        sa.Column("provider_id", sa.Integer(), nullable=False),
        sa.Column("initiated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("trigger_source", sa.String(length=32), nullable=False),
        sa.Column("used_local_prices_only", sa.Boolean(), nullable=False),
        sa.Column("settings_snapshot", sa.JSON(), nullable=False),
        sa.Column("summary_snapshot", sa.JSON(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["initiated_by_user_id"],
            ["app_user.id"],
        ),
        sa.ForeignKeyConstraint(
            ["provider_config_id"],
            ["providerpricelistconfig.id"],
        ),
        sa.ForeignKeyConstraint(
            ["provider_id"],
            ["provider.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_autopurchaserun_provider_config_id"),
        "autopurchaserun",
        ["provider_config_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserun_provider_id"),
        "autopurchaserun",
        ["provider_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserun_initiated_by_user_id"),
        "autopurchaserun",
        ["initiated_by_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserun_status"),
        "autopurchaserun",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserun_mode"),
        "autopurchaserun",
        ["mode"],
        unique=False,
    )

    op.create_table(
        "autopurchaserunitem",
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("selected_supplier_id", sa.Integer(), nullable=True),
        sa.Column("oem_number", sa.String(length=64), nullable=False),
        sa.Column("brand_name", sa.String(length=80), nullable=True),
        sa.Column("autopart_name", sa.String(length=64), nullable=True),
        sa.Column("current_quantity", sa.Integer(), nullable=False),
        sa.Column("latest_price", sa.DECIMAL(precision=10, scale=2), nullable=True),
        sa.Column("minimum_balance", sa.Integer(), nullable=False),
        sa.Column("multiplicity", sa.Integer(), nullable=False),
        sa.Column("in_transit_qty", sa.Integer(), nullable=False),
        sa.Column("sold_last_30_days", sa.Integer(), nullable=False),
        sa.Column("sold_last_90_days", sa.Integer(), nullable=False),
        sa.Column("avg_daily_30", sa.DECIMAL(precision=10, scale=2), nullable=True),
        sa.Column("avg_daily_90", sa.DECIMAL(precision=10, scale=2), nullable=True),
        sa.Column(
            "avg_daily_blended", sa.DECIMAL(precision=10, scale=2), nullable=True
        ),
        sa.Column("estimated_days_left_30_days", sa.Integer(), nullable=True),
        sa.Column(
            "average_actual_lead_days",
            sa.DECIMAL(precision=10, scale=2),
            nullable=True,
        ),
        sa.Column(
            "lead_time_days_used",
            sa.DECIMAL(precision=10, scale=2),
            nullable=True,
        ),
        sa.Column("safety_stock_days", sa.Integer(), nullable=True),
        sa.Column(
            "safety_stock_qty", sa.DECIMAL(precision=10, scale=2), nullable=True
        ),
        sa.Column("reorder_point", sa.DECIMAL(precision=10, scale=2), nullable=True),
        sa.Column("target_stock", sa.Integer(), nullable=True),
        sa.Column("recommended_order_qty", sa.Integer(), nullable=False),
        sa.Column("decision_status", sa.String(length=32), nullable=False),
        sa.Column("autopurchase_mode", sa.String(length=32), nullable=False),
        sa.Column("missing_in_latest_pricelist", sa.Boolean(), nullable=False),
        sa.Column("reason_codes", sa.JSON(), nullable=False),
        sa.Column("reason_titles", sa.JSON(), nullable=False),
        sa.Column("reasons", sa.JSON(), nullable=False),
        sa.Column("abc_xyz", sa.JSON(), nullable=True),
        sa.Column("best_supplier_by_price", sa.JSON(), nullable=True),
        sa.Column("best_supplier_by_lead_time", sa.JSON(), nullable=True),
        sa.Column("recommended_supplier", sa.JSON(), nullable=True),
        sa.Column("draft_purchase_order", sa.JSON(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["autopart_id"], ["autopart.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["autopurchaserun.id"]),
        sa.ForeignKeyConstraint(["selected_supplier_id"], ["provider.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_run_id"),
        "autopurchaserunitem",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_autopart_id"),
        "autopurchaserunitem",
        ["autopart_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_selected_supplier_id"),
        "autopurchaserunitem",
        ["selected_supplier_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_oem_number"),
        "autopurchaserunitem",
        ["oem_number"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_brand_name"),
        "autopurchaserunitem",
        ["brand_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_decision_status"),
        "autopurchaserunitem",
        ["decision_status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_autopurchaserunitem_autopurchase_mode"),
        "autopurchaserunitem",
        ["autopurchase_mode"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_autopurchaserunitem_autopurchase_mode"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_decision_status"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_brand_name"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_oem_number"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_selected_supplier_id"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_autopart_id"),
        table_name="autopurchaserunitem",
    )
    op.drop_index(
        op.f("ix_autopurchaserunitem_run_id"),
        table_name="autopurchaserunitem",
    )
    op.drop_table("autopurchaserunitem")

    op.drop_index(op.f("ix_autopurchaserun_mode"), table_name="autopurchaserun")
    op.drop_index(op.f("ix_autopurchaserun_status"), table_name="autopurchaserun")
    op.drop_index(
        op.f("ix_autopurchaserun_initiated_by_user_id"),
        table_name="autopurchaserun",
    )
    op.drop_index(
        op.f("ix_autopurchaserun_provider_id"),
        table_name="autopurchaserun",
    )
    op.drop_index(
        op.f("ix_autopurchaserun_provider_config_id"),
        table_name="autopurchaserun",
    )
    op.drop_table("autopurchaserun")
