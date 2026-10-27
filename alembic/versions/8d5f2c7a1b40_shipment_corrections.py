"""Add separate corrections of posted customer shipments.

Revision ID: 8d5f2c7a1b40
Revises: 7c4e1a9b2d30
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "8d5f2c7a1b40"
down_revision = "7c4e1a9b2d30"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "shipmentcorrection",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("shipment_id", sa.Integer(), sa.ForeignKey(
            "shipmentdocument.id", ondelete="RESTRICT"
        ), nullable=False),
        sa.Column("doc_number", sa.String(100), nullable=False),
        sa.Column("doc_date", sa.Date(), nullable=False),
        sa.Column("basis_number", sa.String(120), nullable=False),
        sa.Column("basis_date", sa.Date(), nullable=False),
        sa.Column("basis_description", sa.String(255), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("document_function", sa.String(16), nullable=False),
        sa.Column("status", sa.Enum(
            "draft", "posted", name="shipmentcorrectionstatus"
        ), nullable=False, server_default="draft"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("posted_at", sa.DateTime(timezone=True)),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey(
            "app_user.id", ondelete="SET NULL"
        )),
        sa.Column("diadoc_outgoing_document_id", sa.Integer(), sa.ForeignKey(
            "diadocoutgoingdocument.id", ondelete="SET NULL"
        )),
        sa.Column("external_id", sa.String(100)),
        sa.Column("sync_status", postgresql.ENUM(
            name="syncstatus", create_type=False
        ), nullable=False, server_default="pending"),
        sa.UniqueConstraint("doc_number", name="uq_shipmentcorrection_number"),
    )
    for name in (
        "shipment_id", "doc_number", "status", "diadoc_outgoing_document_id",
        "external_id", "sync_status",
    ):
        op.create_index(f"ix_shipmentcorrection_{name}", "shipmentcorrection", [name])

    op.create_table(
        "shipmentcorrectionitem",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("correction_id", sa.Integer(), sa.ForeignKey(
            "shipmentcorrection.id", ondelete="CASCADE"
        ), nullable=False),
        sa.Column("shipment_item_id", sa.Integer(), sa.ForeignKey(
            "shipmentdocumentitem.id", ondelete="RESTRICT"
        ), nullable=False),
        sa.Column("original_number", sa.Integer(), nullable=False),
        sa.Column("product_name", sa.String(512), nullable=False),
        sa.Column("vendor_code", sa.String(255)),
        sa.Column("quantity_before", sa.Integer(), nullable=False),
        sa.Column("quantity_after", sa.Integer(), nullable=False),
        sa.Column("price_before", sa.DECIMAL(12, 2), nullable=False),
        sa.Column("price_after", sa.DECIMAL(12, 2), nullable=False),
        sa.Column("vat_rate", sa.DECIMAL(5, 2), nullable=False),
        sa.UniqueConstraint(
            "correction_id", "shipment_item_id", name="uq_shipmentcorrection_item"
        ),
        sa.CheckConstraint(
            "quantity_before >= 0 AND quantity_after >= 0",
            name="ck_shipmentcorrection_item_qty",
        ),
        sa.CheckConstraint(
            "price_before >= 0 AND price_after >= 0",
            name="ck_shipmentcorrection_item_price",
        ),
    )
    op.create_index(
        "ix_shipmentcorrectionitem_correction_id", "shipmentcorrectionitem",
        ["correction_id"],
    )
    op.create_index(
        "ix_shipmentcorrectionitem_shipment_item_id", "shipmentcorrectionitem",
        ["shipment_item_id"],
    )


def downgrade():
    op.drop_table("shipmentcorrectionitem")
    op.drop_table("shipmentcorrection")
    postgresql.ENUM(name="shipmentcorrectionstatus").drop(
        op.get_bind(), checkfirst=True
    )
