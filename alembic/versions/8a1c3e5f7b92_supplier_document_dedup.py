"""Supplier document dedup between email and Diadoc.

Revision ID: 8a1c3e5f7b92
Revises: 7f3a9c2e5d18
"""

import sqlalchemy as sa

from alembic import op

revision = "8a1c3e5f7b92"
down_revision = "7f3a9c2e5d18"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("supplierordermessage", sa.Column("document_kind", sa.String(32)))
    op.add_column("supplierordermessage", sa.Column("document_key", sa.String(255)))
    op.add_column("supplierordermessage", sa.Column("document_total", sa.DECIMAL(14, 2)))
    op.add_column("supplierordermessage", sa.Column("duplicate_of_diadoc_id", sa.Integer()))
    op.create_index(
        "ix_supplierordermessage_document_key", "supplierordermessage", ["document_key"]
    )
    op.create_index(
        "ix_supplierordermessage_duplicate_of_diadoc_id",
        "supplierordermessage",
        ["duplicate_of_diadoc_id"],
    )

    t = "diadocincomingdocument"
    op.add_column(t, sa.Column("document_kind", sa.String(32)))
    op.add_column(t, sa.Column("dedup_key", sa.String(255)))
    op.add_column(t, sa.Column("total_amount", sa.Numeric(14, 2)))
    op.add_column(t, sa.Column("duplicate_kind", sa.String(32)))
    op.add_column(
        t,
        sa.Column(
            "matched_message_id",
            sa.Integer(),
            sa.ForeignKey("supplierordermessage.id", ondelete="SET NULL"),
        ),
    )
    op.add_column(
        t,
        sa.Column(
            "duplicate_of_document_id",
            sa.Integer(),
            sa.ForeignKey("diadocincomingdocument.id", ondelete="SET NULL"),
        ),
    )
    op.add_column(t, sa.Column("amount_mismatch_status", sa.String(32)))
    op.add_column(
        t,
        sa.Column(
            "mismatch_message_id",
            sa.Integer(),
            sa.ForeignKey("supplierordermessage.id", ondelete="SET NULL"),
        ),
    )
    op.add_column(t, sa.Column("mismatch_other_total", sa.Numeric(14, 2)))
    op.add_column(t, sa.Column("mismatch_resolved_at", sa.DateTime(timezone=True)))
    op.add_column(t, sa.Column("mismatch_log", sa.JSON()))
    op.create_index("ix_diadocincomingdocument_dedup_key", t, ["dedup_key"])
    op.create_index("ix_diadocincomingdocument_matched_message_id", t, ["matched_message_id"])
    op.create_index(
        "ix_diadocincomingdocument_amount_mismatch_status", t, ["amount_mismatch_status"]
    )


def downgrade():
    t = "diadocincomingdocument"
    for name in (
        "ix_diadocincomingdocument_amount_mismatch_status",
        "ix_diadocincomingdocument_matched_message_id",
        "ix_diadocincomingdocument_dedup_key",
    ):
        op.drop_index(name, table_name=t)
    for column in (
        "mismatch_log", "mismatch_resolved_at", "mismatch_other_total",
        "mismatch_message_id", "amount_mismatch_status", "duplicate_of_document_id",
        "matched_message_id", "duplicate_kind", "total_amount", "dedup_key",
        "document_kind",
    ):
        op.drop_column(t, column)
    op.drop_index("ix_supplierordermessage_duplicate_of_diadoc_id", table_name="supplierordermessage")
    op.drop_index("ix_supplierordermessage_document_key", table_name="supplierordermessage")
    for column in ("duplicate_of_diadoc_id", "document_total", "document_key", "document_kind"):
        op.drop_column("supplierordermessage", column)
