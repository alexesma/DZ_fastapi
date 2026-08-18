"""add cross-docking acceptance labels

Revision ID: e1a7c5d9b320
Revises: d8f2c6a4e710
Create Date: 2026-08-10 21:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "e1a7c5d9b320"
down_revision: Union[str, Sequence[str], None] = "d8f2c6a4e710"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    postgresql.ENUM(
        "received",
        "label_pending",
        "ready_for_customer",
        name="crossdockingitemstatus",
    ).create(bind, checkfirst=True)
    postgresql.ENUM(
        "pending",
        "printed",
        name="crossdockinglabelstatus",
    ).create(bind, checkfirst=True)
    item_status = postgresql.ENUM(
        "received",
        "label_pending",
        "ready_for_customer",
        name="crossdockingitemstatus",
        create_type=False,
    )
    label_status = postgresql.ENUM(
        "pending",
        "printed",
        name="crossdockinglabelstatus",
        create_type=False,
    )

    op.add_column(
        "supplierreceiptitem",
        sa.Column("cross_docking_status", item_status, nullable=True),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column(
            "document_pending",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column("accepted_by_user_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierreceiptitem",
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_supplierreceiptitem_accepted_by_user",
        "supplierreceiptitem",
        "app_user",
        ["accepted_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_supplierreceiptitem_cross_docking_status",
        "supplierreceiptitem",
        ["cross_docking_status"],
    )

    op.create_table(
        "crossdockinglabel",
        sa.Column("supplier_receipt_item_id", sa.Integer(), nullable=False),
        sa.Column("stock_order_item_id", sa.Integer(), nullable=True),
        sa.Column("customer_order_item_id", sa.Integer(), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("requested_brand", sa.String(length=255), nullable=False),
        sa.Column("requested_oem", sa.String(length=255), nullable=False),
        sa.Column("requested_name", sa.String(length=512), nullable=True),
        sa.Column("customer_name", sa.String(length=255), nullable=True),
        sa.Column("order_number", sa.String(length=255), nullable=True),
        sa.Column("order_date", sa.Date(), nullable=True),
        sa.Column("barcode", sa.String(length=128), nullable=False),
        sa.Column(
            "status",
            label_status,
            server_default="pending",
            nullable=False,
        ),
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
        sa.CheckConstraint("quantity > 0", name="ck_cross_docking_label_qty"),
        sa.ForeignKeyConstraint(
            ["supplier_receipt_item_id"],
            ["supplierreceiptitem.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["stock_order_item_id"],
            ["stockorderitem.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["customer_order_item_id"],
            ["customerorderitem.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["last_printed_by_user_id"],
            ["app_user.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "supplier_receipt_item_id",
        "stock_order_item_id",
        "customer_order_item_id",
        "barcode",
        "status",
    ):
        op.create_index(
            f"ix_crossdockinglabel_{column}",
            "crossdockinglabel",
            [column],
            unique=column in {"supplier_receipt_item_id", "barcode"},
        )

    op.create_table(
        "crossdockinglabelprintevent",
        sa.Column("label_id", sa.Integer(), nullable=False),
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
            name="ck_cross_docking_label_print_number",
        ),
        sa.ForeignKeyConstraint(
            ["label_id"],
            ["crossdockinglabel.id"],
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
            name="uq_cross_docking_label_print_number",
        ),
    )
    op.create_index(
        "ix_crossdockinglabelprintevent_label_id",
        "crossdockinglabelprintevent",
        ["label_id"],
    )


def downgrade() -> None:
    op.drop_table("crossdockinglabelprintevent")
    op.drop_table("crossdockinglabel")
    op.drop_index(
        "ix_supplierreceiptitem_cross_docking_status",
        table_name="supplierreceiptitem",
    )
    op.drop_constraint(
        "fk_supplierreceiptitem_accepted_by_user",
        "supplierreceiptitem",
        type_="foreignkey",
    )
    op.drop_column("supplierreceiptitem", "ready_at")
    op.drop_column("supplierreceiptitem", "accepted_by_user_id")
    op.drop_column("supplierreceiptitem", "accepted_at")
    op.drop_column("supplierreceiptitem", "document_pending")
    op.drop_column("supplierreceiptitem", "cross_docking_status")
    postgresql.ENUM(name="crossdockinglabelstatus").drop(
        op.get_bind(),
        checkfirst=True,
    )
    postgresql.ENUM(name="crossdockingitemstatus").drop(
        op.get_bind(),
        checkfirst=True,
    )
