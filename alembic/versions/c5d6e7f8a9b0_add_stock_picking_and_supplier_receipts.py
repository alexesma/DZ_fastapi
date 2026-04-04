"""Add stock picking and supplier receipt structures

Revision ID: c5d6e7f8a9b0
Revises: b3d4e5f6a7b8
Create Date: 2026-04-04 17:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c5d6e7f8a9b0"
down_revision: Union[str, None] = "b3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "stockorderitem",
        sa.Column(
            "picked_quantity",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "stockorderitem",
        sa.Column("picked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "stockorderitem",
        sa.Column("picked_by_user_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "stockorderitem",
        sa.Column("pick_comment", sa.String(length=500), nullable=True),
    )
    op.add_column(
        "stockorderitem",
        sa.Column("pick_last_scan_code", sa.String(length=255), nullable=True),
    )
    op.create_foreign_key(
        "fk_stockorderitem_picked_by_user",
        "stockorderitem",
        "app_user",
        ["picked_by_user_id"],
        ["id"],
    )

    op.add_column(
        "supplierorder",
        sa.Column("response_status_raw", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "supplierorder",
        sa.Column(
            "response_status_normalized",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "supplierorder",
        sa.Column(
            "response_status_synced_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_supplierorder_response_status_normalized",
        "supplierorder",
        ["response_status_normalized"],
    )

    op.add_column(
        "supplierorderitem",
        sa.Column("confirmed_quantity", sa.Integer(), nullable=True),
    )
    op.add_column(
        "supplierorderitem",
        sa.Column("response_price", sa.DECIMAL(10, 2), nullable=True),
    )
    op.add_column(
        "supplierorderitem",
        sa.Column("response_comment", sa.String(length=500), nullable=True),
    )
    op.add_column(
        "supplierorderitem",
        sa.Column("response_status_raw", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "supplierorderitem",
        sa.Column(
            "response_status_normalized",
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        "supplierorderitem",
        sa.Column(
            "response_status_synced_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_supplierorderitem_response_status_normalized",
        "supplierorderitem",
        ["response_status_normalized"],
    )

    op.create_table(
        "supplierordermessage",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("supplier_order_id", sa.Integer(), nullable=True),
        sa.Column("provider_id", sa.Integer(), nullable=False),
        sa.Column("message_type", sa.String(length=32), nullable=False),
        sa.Column("subject", sa.String(length=500), nullable=True),
        sa.Column("sender_email", sa.String(length=255), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column("body_preview", sa.Text(), nullable=True),
        sa.Column("raw_status", sa.String(length=255), nullable=True),
        sa.Column(
            "normalized_status",
            sa.String(length=255),
            nullable=True,
        ),
        sa.Column("parse_confidence", sa.Float(), nullable=True),
        sa.Column("source_uid", sa.String(length=128), nullable=True),
        sa.Column(
            "source_message_id",
            sa.String(length=255),
            nullable=True,
        ),
        sa.Column("mapping_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["supplier_order_id"], ["supplierorder.id"]),
        sa.ForeignKeyConstraint(["provider_id"], ["provider.id"]),
        sa.ForeignKeyConstraint(
            ["mapping_id"],
            ["external_status_mapping.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_supplierordermessage_supplier_order_id",
        "supplierordermessage",
        ["supplier_order_id"],
    )
    op.create_index(
        "ix_supplierordermessage_normalized_status",
        "supplierordermessage",
        ["normalized_status"],
    )
    op.create_index(
        "ix_supplierordermessage_source_uid",
        "supplierordermessage",
        ["source_uid"],
    )
    op.create_index(
        "ix_supplierordermessage_source_message_id",
        "supplierordermessage",
        ["source_message_id"],
    )

    op.create_table(
        "supplierorderattachment",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("message_id", sa.Integer(), nullable=False),
        sa.Column("filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("file_path", sa.String(length=1024), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("parsed_kind", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["message_id"],
            ["supplierordermessage.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_supplierorderattachment_message_id",
        "supplierorderattachment",
        ["message_id"],
    )
    op.create_index(
        "ix_supplierorderattachment_sha256",
        "supplierorderattachment",
        ["sha256"],
    )

    op.create_table(
        "supplierreceipt",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("provider_id", sa.Integer(), nullable=False),
        sa.Column("supplier_order_id", sa.Integer(), nullable=True),
        sa.Column("source_message_id", sa.Integer(), nullable=True),
        sa.Column(
            "document_number",
            sa.String(length=120),
            nullable=True,
        ),
        sa.Column("document_date", sa.Date(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["provider_id"], ["provider.id"]),
        sa.ForeignKeyConstraint(["supplier_order_id"], ["supplierorder.id"]),
        sa.ForeignKeyConstraint(
            ["source_message_id"],
            ["supplierordermessage.id"],
        ),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["app_user.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_supplierreceipt_supplier_order_id",
        "supplierreceipt",
        ["supplier_order_id"],
    )
    op.create_index(
        "ix_supplierreceipt_source_message_id",
        "supplierreceipt",
        ["source_message_id"],
    )
    op.create_index(
        "ix_supplierreceipt_document_number",
        "supplierreceipt",
        ["document_number"],
    )
    op.create_index(
        "ix_supplierreceipt_document_date",
        "supplierreceipt",
        ["document_date"],
    )

    op.create_table(
        "supplierreceiptitem",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("receipt_id", sa.Integer(), nullable=False),
        sa.Column("supplier_order_id", sa.Integer(), nullable=True),
        sa.Column("supplier_order_item_id", sa.Integer(), nullable=True),
        sa.Column("customer_order_item_id", sa.Integer(), nullable=True),
        sa.Column("autopart_id", sa.Integer(), nullable=True),
        sa.Column("oem_number", sa.String(length=120), nullable=True),
        sa.Column("brand_name", sa.String(length=120), nullable=True),
        sa.Column("autopart_name", sa.String(length=512), nullable=True),
        sa.Column("ordered_quantity", sa.Integer(), nullable=True),
        sa.Column("confirmed_quantity", sa.Integer(), nullable=True),
        sa.Column(
            "received_quantity",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("price", sa.DECIMAL(10, 2), nullable=True),
        sa.Column("comment", sa.String(length=500), nullable=True),
        sa.ForeignKeyConstraint(
            ["receipt_id"],
            ["supplierreceipt.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["supplier_order_id"], ["supplierorder.id"]),
        sa.ForeignKeyConstraint(
            ["supplier_order_item_id"],
            ["supplierorderitem.id"],
        ),
        sa.ForeignKeyConstraint(
            ["customer_order_item_id"],
            ["customerorderitem.id"],
        ),
        sa.ForeignKeyConstraint(["autopart_id"], ["autopart.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_supplierreceiptitem_receipt_id",
        "supplierreceiptitem",
        ["receipt_id"],
    )
    op.create_index(
        "ix_supplierreceiptitem_supplier_order_id",
        "supplierreceiptitem",
        ["supplier_order_id"],
    )
    op.create_index(
        "ix_supplierreceiptitem_supplier_order_item_id",
        "supplierreceiptitem",
        ["supplier_order_item_id"],
    )
    op.create_index(
        "ix_supplierreceiptitem_customer_order_item_id",
        "supplierreceiptitem",
        ["customer_order_item_id"],
    )
    op.create_index(
        "ix_supplierreceiptitem_oem_number",
        "supplierreceiptitem",
        ["oem_number"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_supplierreceiptitem_oem_number",
        table_name="supplierreceiptitem",
    )
    op.drop_index(
        "ix_supplierreceiptitem_customer_order_item_id",
        table_name="supplierreceiptitem",
    )
    op.drop_index(
        "ix_supplierreceiptitem_supplier_order_item_id",
        table_name="supplierreceiptitem",
    )
    op.drop_index(
        "ix_supplierreceiptitem_supplier_order_id",
        table_name="supplierreceiptitem",
    )
    op.drop_index(
        "ix_supplierreceiptitem_receipt_id",
        table_name="supplierreceiptitem",
    )
    op.drop_table("supplierreceiptitem")

    op.drop_index(
        "ix_supplierreceipt_document_date",
        table_name="supplierreceipt",
    )
    op.drop_index(
        "ix_supplierreceipt_document_number",
        table_name="supplierreceipt",
    )
    op.drop_index(
        "ix_supplierreceipt_source_message_id",
        table_name="supplierreceipt",
    )
    op.drop_index(
        "ix_supplierreceipt_supplier_order_id",
        table_name="supplierreceipt",
    )
    op.drop_table("supplierreceipt")

    op.drop_index(
        "ix_supplierorderattachment_sha256",
        table_name="supplierorderattachment",
    )
    op.drop_index(
        "ix_supplierorderattachment_message_id",
        table_name="supplierorderattachment",
    )
    op.drop_table("supplierorderattachment")

    op.drop_index(
        "ix_supplierordermessage_source_message_id",
        table_name="supplierordermessage",
    )
    op.drop_index(
        "ix_supplierordermessage_source_uid",
        table_name="supplierordermessage",
    )
    op.drop_index(
        "ix_supplierordermessage_normalized_status",
        table_name="supplierordermessage",
    )
    op.drop_index(
        "ix_supplierordermessage_supplier_order_id",
        table_name="supplierordermessage",
    )
    op.drop_table("supplierordermessage")

    op.drop_index(
        "ix_supplierorderitem_response_status_normalized",
        table_name="supplierorderitem",
    )
    op.drop_column("supplierorderitem", "response_status_synced_at")
    op.drop_column("supplierorderitem", "response_status_normalized")
    op.drop_column("supplierorderitem", "response_status_raw")
    op.drop_column("supplierorderitem", "response_comment")
    op.drop_column("supplierorderitem", "response_price")
    op.drop_column("supplierorderitem", "confirmed_quantity")

    op.drop_index(
        "ix_supplierorder_response_status_normalized",
        table_name="supplierorder",
    )
    op.drop_column("supplierorder", "response_status_synced_at")
    op.drop_column("supplierorder", "response_status_normalized")
    op.drop_column("supplierorder", "response_status_raw")

    op.drop_constraint(
        "fk_stockorderitem_picked_by_user",
        "stockorderitem",
        type_="foreignkey",
    )
    op.drop_column("stockorderitem", "pick_last_scan_code")
    op.drop_column("stockorderitem", "pick_comment")
    op.drop_column("stockorderitem", "picked_by_user_id")
    op.drop_column("stockorderitem", "picked_at")
    op.drop_column("stockorderitem", "picked_quantity")
