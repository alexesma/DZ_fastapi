"""add DragonZap production waves

Revision ID: 9e4a7c3d1b62
Revises: 8d3f6b2c0a51
Create Date: 2026-08-09 21:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "9e4a7c3d1b62"
down_revision: Union[str, Sequence[str], None] = "8d3f6b2c0a51"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    wave_status_values = (
        "draft",
        "planned",
        "in_progress",
        "completed",
        "cancelled",
    )
    wave_source_values = (
        "manual",
        "scheduled",
    )
    postgresql.ENUM(
        *wave_status_values,
        name="productionwavestatus",
    ).create(bind, checkfirst=True)
    postgresql.ENUM(
        *wave_source_values,
        name="productionwavesource",
    ).create(bind, checkfirst=True)
    wave_status = postgresql.ENUM(
        *wave_status_values,
        name="productionwavestatus",
        create_type=False,
    )
    wave_source = postgresql.ENUM(
        *wave_source_values,
        name="productionwavesource",
        create_type=False,
    )

    op.execute("ALTER TYPE movementtype ADD VALUE IF NOT EXISTS 'production_consume'")
    op.execute("ALTER TYPE movementtype ADD VALUE IF NOT EXISTS 'production_output'")
    op.execute("ALTER TYPE lotsourcetype ADD VALUE IF NOT EXISTS 'production'")
    op.execute("ALTER TYPE markingmovementtype ADD VALUE IF NOT EXISTS 'production'")

    op.create_table(
        "productionwave",
        sa.Column("number", sa.String(length=64), nullable=True),
        sa.Column("warehouse_id", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            wave_status,
            nullable=False,
            server_default="draft",
        ),
        sa.Column(
            "source",
            wave_source,
            nullable=False,
            server_default="manual",
        ),
        sa.Column("cutoff_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("total_planned_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("total_produced_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("total_material_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("total_packaging_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("total_finished_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("planned_by_user_id", sa.Integer(), nullable=True),
        sa.Column("started_by_user_id", sa.Integer(), nullable=True),
        sa.Column("completed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("cancelled_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("planned_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("external_id", sa.String(length=100), nullable=True),
        sa.Column(
            "sync_status",
            postgresql.ENUM(name="syncstatus", create_type=False),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["warehouse_id"], ["warehouse.id"], ondelete="RESTRICT"),
        *[
            sa.ForeignKeyConstraint([column], ["app_user.id"], ondelete="SET NULL")
            for column in (
                "created_by_user_id",
                "planned_by_user_id",
                "started_by_user_id",
                "completed_by_user_id",
                "cancelled_by_user_id",
            )
        ],
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "number",
        "warehouse_id",
        "status",
        "cutoff_at",
        "external_id",
        "sync_status",
    ):
        op.create_index(
            f"ix_productionwave_{column}",
            "productionwave",
            [column],
            unique=column == "number",
        )

    op.create_table(
        "productionwaveitem",
        sa.Column("wave_id", sa.Integer(), nullable=False),
        sa.Column("production_group_id", sa.Integer(), nullable=False),
        sa.Column("finished_autopart_id", sa.Integer(), nullable=False),
        sa.Column("planned_quantity", sa.Integer(), nullable=False),
        sa.Column("produced_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("shortage_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("planning_error", sa.Text(), nullable=True),
        sa.Column("material_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("packaging_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("total_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("unit_cost", sa.DECIMAL(12, 4), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint("planned_quantity > 0", name="ck_production_wave_item_qty"),
        sa.ForeignKeyConstraint(["wave_id"], ["productionwave.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["production_group_id"],
            ["dragonzapproductiongroup.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["finished_autopart_id"], ["autopart.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("wave_id", "production_group_id", name="uq_production_wave_item_group"),
    )
    for column in ("wave_id", "production_group_id", "finished_autopart_id"):
        op.create_index(
            f"ix_productionwaveitem_{column}",
            "productionwaveitem",
            [column],
        )

    op.create_table(
        "productionwavedemand",
        sa.Column("wave_item_id", sa.Integer(), nullable=False),
        sa.Column("customer_order_item_id", sa.Integer(), nullable=False),
        sa.Column("stock_order_item_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("customer_id", sa.Integer(), nullable=True),
        sa.Column("customer_name", sa.String(length=255), nullable=True),
        sa.Column("customer_order_id", sa.Integer(), nullable=True),
        sa.Column("order_number", sa.String(length=255), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("requested_brand", sa.String(length=255), nullable=False),
        sa.Column("requested_oem", sa.String(length=255), nullable=False),
        sa.Column("requested_name", sa.String(length=512), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_production_wave_demand_qty"),
        sa.ForeignKeyConstraint(["wave_item_id"], ["productionwaveitem.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["customer_order_item_id"], ["customerorderitem.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["stock_order_item_id"], ["stockorderitem.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["customer_id"], ["customer.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["customer_order_id"], ["customerorder.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "wave_item_id",
            "stock_order_item_id",
            name="uq_production_wave_demand_stock_item",
        ),
    )
    for column in ("wave_item_id", "customer_order_item_id", "stock_order_item_id"):
        op.create_index(
            f"ix_productionwavedemand_{column}",
            "productionwavedemand",
            [column],
        )

    op.create_table(
        "productionwaveallocation",
        sa.Column("wave_item_id", sa.Integer(), nullable=False),
        sa.Column("material_autopart_id", sa.Integer(), nullable=False),
        sa.Column("stock_lot_id", sa.Integer(), nullable=False),
        sa.Column("storage_location_id", sa.Integer(), nullable=False),
        sa.Column("output_stock_lot_id", sa.Integer(), nullable=True),
        sa.Column("planned_quantity", sa.Integer(), nullable=False),
        sa.Column("consumed_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("unit_material_cost", sa.DECIMAL(12, 4), nullable=True),
        sa.Column("total_material_cost", sa.DECIMAL(14, 2), server_default="0", nullable=False),
        sa.Column("gtd_number", sa.String(length=64), nullable=True),
        sa.Column("country_code", sa.String(length=16), nullable=True),
        sa.Column("country_name", sa.String(length=120), nullable=True),
        sa.Column("marking_codes", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint("planned_quantity > 0", name="ck_production_wave_allocation_qty"),
        sa.ForeignKeyConstraint(["wave_item_id"], ["productionwaveitem.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["material_autopart_id"], ["autopart.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["stock_lot_id"], ["stocklot.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["storage_location_id"], ["storagelocation.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["output_stock_lot_id"], ["stocklot.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "wave_item_id", "stock_lot_id", name="uq_production_wave_allocation_lot"
        ),
    )
    for column in (
        "wave_item_id",
        "material_autopart_id",
        "stock_lot_id",
        "storage_location_id",
        "output_stock_lot_id",
    ):
        op.create_index(
            f"ix_productionwaveallocation_{column}",
            "productionwaveallocation",
            [column],
            unique=column == "output_stock_lot_id",
        )


def downgrade() -> None:
    op.drop_table("productionwaveallocation")
    op.drop_table("productionwavedemand")
    op.drop_table("productionwaveitem")
    op.drop_table("productionwave")
    postgresql.ENUM(name="productionwavesource").drop(op.get_bind(), checkfirst=True)
    postgresql.ENUM(name="productionwavestatus").drop(op.get_bind(), checkfirst=True)
    # PostgreSQL cannot remove values from existing enums safely. The added
    # movement/source values are intentionally retained after downgrade.
