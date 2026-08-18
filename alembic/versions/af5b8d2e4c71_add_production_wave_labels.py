"""add production wave labels

Revision ID: af5b8d2e4c71
Revises: 9e4a7c3d1b62
Create Date: 2026-08-10 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "af5b8d2e4c71"
down_revision: Union[str, Sequence[str], None] = "9e4a7c3d1b62"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    postgresql.ENUM(
        "pending",
        "printed",
        name="productionwavelabelstatus",
    ).create(bind, checkfirst=True)
    label_status = postgresql.ENUM(
        "pending",
        "printed",
        name="productionwavelabelstatus",
        create_type=False,
    )
    op.create_table(
        "productionwavelabel",
        sa.Column("wave_id", sa.Integer(), nullable=False),
        sa.Column("wave_item_id", sa.Integer(), nullable=False),
        sa.Column("wave_demand_id", sa.Integer(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("total_labels", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Integer(), server_default="1", nullable=False),
        sa.Column("requested_brand", sa.String(length=255), nullable=False),
        sa.Column("requested_oem", sa.String(length=255), nullable=False),
        sa.Column("requested_name", sa.String(length=512), nullable=True),
        sa.Column("customer_name", sa.String(length=255), nullable=True),
        sa.Column("order_number", sa.String(length=255), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("barcode", sa.String(length=128), nullable=False),
        sa.Column("status", label_status, server_default="pending", nullable=False),
        sa.Column("print_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_printed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_printed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("last_print_reason", sa.String(length=500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_production_wave_label_qty"),
        sa.CheckConstraint(
            "sequence_number > 0",
            name="ck_production_wave_label_sequence",
        ),
        sa.CheckConstraint(
            "total_labels > 0",
            name="ck_production_wave_label_total",
        ),
        sa.ForeignKeyConstraint(["wave_id"], ["productionwave.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["wave_item_id"],
            ["productionwaveitem.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["wave_demand_id"],
            ["productionwavedemand.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["last_printed_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "wave_demand_id",
            "sequence_number",
            name="uq_production_wave_label_demand_sequence",
        ),
    )
    for column in (
        "wave_id",
        "wave_item_id",
        "wave_demand_id",
        "barcode",
        "status",
    ):
        op.create_index(
            f"ix_productionwavelabel_{column}",
            "productionwavelabel",
            [column],
            unique=column == "barcode",
        )
    op.create_table(
        "productionwavelabelprintevent",
        sa.Column("label_id", sa.Integer(), nullable=False),
        sa.Column("wave_id", sa.Integer(), nullable=False),
        sa.Column("print_number", sa.Integer(), nullable=False),
        sa.Column("printed_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "printed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("reason", sa.String(length=500), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "print_number > 0",
            name="ck_production_wave_label_print_number",
        ),
        sa.ForeignKeyConstraint(
            ["label_id"],
            ["productionwavelabel.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["wave_id"],
            ["productionwave.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["printed_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "label_id",
            "print_number",
            name="uq_production_wave_label_print_number",
        ),
    )
    op.create_index(
        "ix_productionwavelabelprintevent_label_id",
        "productionwavelabelprintevent",
        ["label_id"],
    )
    op.create_index(
        "ix_productionwavelabelprintevent_wave_id",
        "productionwavelabelprintevent",
        ["wave_id"],
    )


def downgrade() -> None:
    op.drop_table("productionwavelabelprintevent")
    op.drop_table("productionwavelabel")
    postgresql.ENUM(name="productionwavelabelstatus").drop(
        op.get_bind(),
        checkfirst=True,
    )
