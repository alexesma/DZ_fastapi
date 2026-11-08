"""Add supplier documents registry and link payments to documents.

Revision ID: d1a2b3c4e5f6
Revises: c0f1a2b3d4e5
Create Date: 2026-11-08 19:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d1a2b3c4e5f6"
down_revision: Union[str, Sequence[str], None] = "c0f1a2b3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "supplierdocument",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "provider_id",
            sa.Integer(),
            sa.ForeignKey("provider.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("doc_type", sa.String(32), nullable=False),
        sa.Column("number", sa.String(100)),
        sa.Column("doc_date", sa.Date()),
        sa.Column("due_date", sa.Date()),
        sa.Column("amount", sa.DECIMAL(12, 2)),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column(
            "diadoc_document_id",
            sa.Integer(),
            sa.ForeignKey("diadocincomingdocument.id", ondelete="SET NULL"),
            unique=True,
        ),
        sa.Column("file_name", sa.String(512)),
        sa.Column("notes", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_supplierdocument_provider_id", "supplierdocument", ["provider_id"]
    )
    op.add_column(
        "supplierpayment",
        sa.Column(
            "supplier_document_id",
            sa.Integer(),
            sa.ForeignKey("supplierdocument.id", ondelete="SET NULL"),
        ),
    )
    op.create_index(
        "ix_supplierpayment_supplier_document_id",
        "supplierpayment",
        ["supplier_document_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_supplierpayment_supplier_document_id", table_name="supplierpayment"
    )
    op.drop_column("supplierpayment", "supplier_document_id")
    op.drop_index(
        "ix_supplierdocument_provider_id", table_name="supplierdocument"
    )
    op.drop_table("supplierdocument")
