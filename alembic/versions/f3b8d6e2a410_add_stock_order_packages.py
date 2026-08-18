"""add stock order packages

Revision ID: f3b8d6e2a410
Revises: e1a7c5d9b320
Create Date: 2026-08-11 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "f3b8d6e2a410"
down_revision: Union[str, Sequence[str], None] = "e1a7c5d9b320"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    postgresql.ENUM(
        "open",
        "sealed",
        "verified",
        name="stockorderpackagestatus",
    ).create(bind, checkfirst=True)
    postgresql.ENUM(
        "created",
        "contents_changed",
        "sealed",
        "scanned",
        "verified",
        "reopened",
        "label_printed",
        name="stockorderpackageeventtype",
    ).create(bind, checkfirst=True)
    package_status = postgresql.ENUM(
        "open",
        "sealed",
        "verified",
        name="stockorderpackagestatus",
        create_type=False,
    )
    event_type = postgresql.ENUM(
        "created",
        "contents_changed",
        "sealed",
        "scanned",
        "verified",
        "reopened",
        "label_printed",
        name="stockorderpackageeventtype",
        create_type=False,
    )

    op.add_column(
        "stockorder",
        sa.Column(
            "packing_required",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.create_table(
        "stockorderpackage",
        sa.Column("stock_order_id", sa.Integer(), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("barcode", sa.String(length=128), nullable=False),
        sa.Column(
            "status",
            package_status,
            server_default="open",
            nullable=False,
        ),
        sa.Column("comment", sa.String(length=500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("sealed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sealed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified_by_user_id", sa.Integer(), nullable=True),
        sa.Column("print_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_printed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_printed_by_user_id", sa.Integer(), nullable=True),
        sa.Column("last_print_reason", sa.String(length=500), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "sequence_number > 0",
            name="ck_stock_order_package_sequence",
        ),
        sa.ForeignKeyConstraint(
            ["stock_order_id"],
            ["stockorder.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["sealed_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["verified_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["last_printed_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "stock_order_id",
            "sequence_number",
            name="uq_stock_order_package_sequence",
        ),
    )
    op.create_index(
        "ix_stockorderpackage_stock_order_id",
        "stockorderpackage",
        ["stock_order_id"],
    )
    op.create_index(
        "ix_stockorderpackage_barcode",
        "stockorderpackage",
        ["barcode"],
        unique=True,
    )
    op.create_index(
        "ix_stockorderpackage_status",
        "stockorderpackage",
        ["status"],
    )

    op.create_table(
        "stockorderpackageitem",
        sa.Column("package_id", sa.Integer(), nullable=False),
        sa.Column("stock_order_item_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("verified_quantity", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_scan_code", sa.String(length=255), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_verified_by_user_id", sa.Integer(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "quantity > 0",
            name="ck_stock_order_package_item_qty",
        ),
        sa.CheckConstraint(
            "verified_quantity >= 0 AND verified_quantity <= quantity",
            name="ck_stock_order_package_item_verified_qty",
        ),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["stockorderpackage.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["stock_order_item_id"],
            ["stockorderitem.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["last_verified_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "package_id",
            "stock_order_item_id",
            name="uq_stock_order_package_item",
        ),
    )
    op.create_index(
        "ix_stockorderpackageitem_package_id",
        "stockorderpackageitem",
        ["package_id"],
    )
    op.create_index(
        "ix_stockorderpackageitem_stock_order_item_id",
        "stockorderpackageitem",
        ["stock_order_item_id"],
    )

    op.create_table(
        "stockorderpackageevent",
        sa.Column("package_id", sa.Integer(), nullable=False),
        sa.Column("event_type", event_type, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("reason", sa.String(length=500), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["stockorderpackage.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_stockorderpackageevent_package_id",
        "stockorderpackageevent",
        ["package_id"],
    )
    op.create_index(
        "ix_stockorderpackageevent_event_type",
        "stockorderpackageevent",
        ["event_type"],
    )


def downgrade() -> None:
    op.drop_table("stockorderpackageevent")
    op.drop_table("stockorderpackageitem")
    op.drop_table("stockorderpackage")
    op.drop_column("stockorder", "packing_required")
    postgresql.ENUM(name="stockorderpackageeventtype").drop(
        op.get_bind(),
        checkfirst=True,
    )
    postgresql.ENUM(name="stockorderpackagestatus").drop(
        op.get_bind(),
        checkfirst=True,
    )
