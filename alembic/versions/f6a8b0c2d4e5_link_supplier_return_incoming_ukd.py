"""link supplier return to incoming Diadoc UKD

Revision ID: f6a8b0c2d4e5
Revises: e5f7a9b1c3d4
Create Date: 2026-11-21 22:30:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "f6a8b0c2d4e5"
down_revision = "e5f7a9b1c3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "returntosupplier",
        sa.Column("diadoc_incoming_document_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "returntosupplier",
        sa.Column("supplier_receipt_correction_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "returntosupplier",
        sa.Column("diadoc_status", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "returntosupplier",
        sa.Column("diadoc_status_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_returntosupplier_diadoc_incoming_document",
        "returntosupplier",
        "diadocincomingdocument",
        ["diadoc_incoming_document_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_returntosupplier_supplier_receipt_correction",
        "returntosupplier",
        "supplierreceiptcorrection",
        ["supplier_receipt_correction_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        op.f("ix_returntosupplier_diadoc_incoming_document_id"),
        "returntosupplier",
        ["diadoc_incoming_document_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_returntosupplier_supplier_receipt_correction_id"),
        "returntosupplier",
        ["supplier_receipt_correction_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_returntosupplier_diadoc_status"),
        "returntosupplier",
        ["diadoc_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_returntosupplier_diadoc_status"),
        table_name="returntosupplier",
    )
    op.drop_index(
        op.f("ix_returntosupplier_supplier_receipt_correction_id"),
        table_name="returntosupplier",
    )
    op.drop_index(
        op.f("ix_returntosupplier_diadoc_incoming_document_id"),
        table_name="returntosupplier",
    )
    op.drop_constraint(
        "fk_returntosupplier_supplier_receipt_correction",
        "returntosupplier",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_returntosupplier_diadoc_incoming_document",
        "returntosupplier",
        type_="foreignkey",
    )
    op.drop_column("returntosupplier", "diadoc_status_updated_at")
    op.drop_column("returntosupplier", "diadoc_status")
    op.drop_column("returntosupplier", "supplier_receipt_correction_id")
    op.drop_column("returntosupplier", "diadoc_incoming_document_id")
