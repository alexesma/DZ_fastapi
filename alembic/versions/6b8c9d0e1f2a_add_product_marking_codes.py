"""add product marking codes

Revision ID: 6b8c9d0e1f2a
Revises: 5472351f4829
Create Date: 2026-07-03 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "6b8c9d0e1f2a"
down_revision: Union[str, Sequence[str], None] = "5472351f4829"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


marking_code_status = postgresql.ENUM(
    "received",
    "in_stock",
    "reserved",
    "shipped",
    "withdrawn",
    "error",
    name="markingcodestatus",
)
marking_movement_type = postgresql.ENUM(
    "received",
    "stocked",
    "reserved",
    "shipped",
    "unposted",
    "gis_mt_reported",
    "error",
    name="markingmovementtype",
)
# В колонках create_table используем create_type=False: типы создаются
# явно в upgrade() (checkfirst), иначе create_table пытается создать их
# повторно и падает с DuplicateObjectError.
marking_code_status_column = postgresql.ENUM(
    name="markingcodestatus",
    create_type=False,
)
marking_movement_type_column = postgresql.ENUM(
    name="markingmovementtype",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    marking_code_status.create(bind, checkfirst=True)
    marking_movement_type.create(bind, checkfirst=True)

    op.add_column(
        "supplierreceiptitem",
        sa.Column("marking_codes", sa.JSON(), nullable=True),
    )
    op.add_column(
        "stocklot",
        sa.Column("marking_codes", sa.JSON(), nullable=True),
    )
    op.add_column(
        "shipmentdocumentitemlotallocation",
        sa.Column("marking_codes", sa.JSON(), nullable=True),
    )

    op.create_table(
        "productmarkingcode",
        sa.Column("code", sa.String(length=255), nullable=False),
        sa.Column(
            "status",
            marking_code_status_column,
            nullable=False,
        ),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("warehouse_id", sa.Integer(), nullable=True),
        sa.Column("storage_location_id", sa.Integer(), nullable=True),
        sa.Column("stock_lot_id", sa.Integer(), nullable=True),
        sa.Column("supplier_receipt_id", sa.Integer(), nullable=True),
        sa.Column("supplier_receipt_item_id", sa.Integer(), nullable=True),
        sa.Column("shipment_document_id", sa.Integer(), nullable=True),
        sa.Column("shipment_document_item_id", sa.Integer(), nullable=True),
        sa.Column("shipment_allocation_id", sa.Integer(), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("shipped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("withdrawn_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["autopart_id"], ["autopart.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["warehouse_id"], ["warehouse.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["storage_location_id"],
            ["storagelocation.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["stock_lot_id"], ["stocklot.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["supplier_receipt_id"],
            ["supplierreceipt.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["supplier_receipt_item_id"],
            ["supplierreceiptitem.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_document_id"],
            ["shipmentdocument.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_document_item_id"],
            ["shipmentdocumentitem.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_allocation_id"],
            ["shipmentdocumentitemlotallocation.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("code"),
    )
    op.create_index(
        op.f("ix_productmarkingcode_code"),
        "productmarkingcode",
        ["code"],
        unique=False,
    )
    for column in (
        "status",
        "autopart_id",
        "warehouse_id",
        "storage_location_id",
        "stock_lot_id",
        "supplier_receipt_id",
        "supplier_receipt_item_id",
        "shipment_document_id",
        "shipment_document_item_id",
        "shipment_allocation_id",
    ):
        op.create_index(
            op.f(f"ix_productmarkingcode_{column}"),
            "productmarkingcode",
            [column],
            unique=False,
        )
    op.create_index(
        "idx_productmarkingcode_status_lot",
        "productmarkingcode",
        ["status", "stock_lot_id"],
        unique=False,
    )
    op.create_index(
        "idx_productmarkingcode_receipt_item",
        "productmarkingcode",
        ["supplier_receipt_item_id", "status"],
        unique=False,
    )

    op.create_table(
        "productmarkingcodemovement",
        sa.Column("marking_code_id", sa.Integer(), nullable=False),
        sa.Column(
            "movement_type",
            marking_movement_type_column,
            nullable=False,
        ),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("stock_lot_id", sa.Integer(), nullable=True),
        sa.Column("supplier_receipt_id", sa.Integer(), nullable=True),
        sa.Column("supplier_receipt_item_id", sa.Integer(), nullable=True),
        sa.Column("shipment_document_id", sa.Integer(), nullable=True),
        sa.Column("shipment_document_item_id", sa.Integer(), nullable=True),
        sa.Column("shipment_allocation_id", sa.Integer(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["marking_code_id"],
            ["productmarkingcode.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["autopart_id"], ["autopart.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["stock_lot_id"], ["stocklot.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["supplier_receipt_id"],
            ["supplierreceipt.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["supplier_receipt_item_id"],
            ["supplierreceiptitem.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_document_id"],
            ["shipmentdocument.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_document_item_id"],
            ["shipmentdocumentitem.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["shipment_allocation_id"],
            ["shipmentdocumentitemlotallocation.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "marking_code_id",
        "movement_type",
        "autopart_id",
        "stock_lot_id",
        "supplier_receipt_id",
        "supplier_receipt_item_id",
        "shipment_document_id",
        "shipment_document_item_id",
        "shipment_allocation_id",
    ):
        op.create_index(
            op.f(f"ix_productmarkingcodemovement_{column}"),
            "productmarkingcodemovement",
            [column],
            unique=False,
        )


def downgrade() -> None:
    for column in (
        "shipment_allocation_id",
        "shipment_document_item_id",
        "shipment_document_id",
        "supplier_receipt_item_id",
        "supplier_receipt_id",
        "stock_lot_id",
        "autopart_id",
        "movement_type",
        "marking_code_id",
    ):
        op.drop_index(
            op.f(f"ix_productmarkingcodemovement_{column}"),
            table_name="productmarkingcodemovement",
        )
    op.drop_table("productmarkingcodemovement")

    op.drop_index(
        "idx_productmarkingcode_receipt_item",
        table_name="productmarkingcode",
    )
    op.drop_index(
        "idx_productmarkingcode_status_lot",
        table_name="productmarkingcode",
    )
    for column in (
        "shipment_allocation_id",
        "shipment_document_item_id",
        "shipment_document_id",
        "supplier_receipt_item_id",
        "supplier_receipt_id",
        "stock_lot_id",
        "storage_location_id",
        "warehouse_id",
        "autopart_id",
        "status",
        "code",
    ):
        op.drop_index(
            op.f(f"ix_productmarkingcode_{column}"),
            table_name="productmarkingcode",
        )
    op.drop_table("productmarkingcode")

    op.drop_column("shipmentdocumentitemlotallocation", "marking_codes")
    op.drop_column("stocklot", "marking_codes")
    op.drop_column("supplierreceiptitem", "marking_codes")

    bind = op.get_bind()
    marking_movement_type.drop(bind, checkfirst=True)
    marking_code_status.drop(bind, checkfirst=True)
