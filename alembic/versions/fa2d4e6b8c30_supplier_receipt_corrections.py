"""Add supplier receipt quantity corrections.

Revision ID: fa2d4e6b8c30
Revises: f9d1e3a5b427
"""

from alembic import op
import sqlalchemy as sa


revision = "fa2d4e6b8c30"
down_revision = "f9d1e3a5b427"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE movementtype ADD VALUE IF NOT EXISTS 'supplier_correction'")
    op.create_table(
        "supplierreceiptcorrection",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider_id", sa.Integer(), sa.ForeignKey("provider.id"), nullable=False),
        sa.Column("receipt_id", sa.Integer(), sa.ForeignKey("supplierreceipt.id")),
        sa.Column(
            "source_message_id",
            sa.Integer(),
            sa.ForeignKey("supplierordermessage.id"),
            unique=True,
        ),
        sa.Column("source_kind", sa.String(16), nullable=False),
        sa.Column("document_number", sa.String(120)),
        sa.Column("document_date", sa.Date()),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("comment", sa.Text()),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("app_user.id")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("posted_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_supplierreceiptcorrection_provider_id", "supplierreceiptcorrection", ["provider_id"])
    op.create_index("ix_supplierreceiptcorrection_receipt_id", "supplierreceiptcorrection", ["receipt_id"])
    op.create_index("ix_supplierreceiptcorrection_status", "supplierreceiptcorrection", ["status"])
    op.create_table(
        "supplierreceiptcorrectionitem",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "correction_id", sa.Integer(),
            sa.ForeignKey("supplierreceiptcorrection.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("receipt_item_id", sa.Integer(), sa.ForeignKey("supplierreceiptitem.id"), nullable=False),
        sa.Column("quantity_before", sa.Integer()),
        sa.Column("quantity_after", sa.Integer(), nullable=False),
        sa.Column("quantity_delta", sa.Integer()),
    )
    op.create_index("ix_supplierreceiptcorrectionitem_correction_id", "supplierreceiptcorrectionitem", ["correction_id"])
    op.create_index("ix_supplierreceiptcorrectionitem_receipt_item_id", "supplierreceiptcorrectionitem", ["receipt_item_id"])


def downgrade():
    op.drop_index("ix_supplierreceiptcorrectionitem_receipt_item_id", "supplierreceiptcorrectionitem")
    op.drop_index("ix_supplierreceiptcorrectionitem_correction_id", "supplierreceiptcorrectionitem")
    op.drop_table("supplierreceiptcorrectionitem")
    op.drop_index("ix_supplierreceiptcorrection_status", "supplierreceiptcorrection")
    op.drop_index("ix_supplierreceiptcorrection_receipt_id", "supplierreceiptcorrection")
    op.drop_index("ix_supplierreceiptcorrection_provider_id", "supplierreceiptcorrection")
    op.drop_table("supplierreceiptcorrection")
    # PostgreSQL enum values cannot be removed without rebuilding the type.
