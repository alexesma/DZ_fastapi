"""shipment documents imported from Diadoc UPD

Revision ID: c0e2f4a6b8d9
Revises: b9d1e3f5a7c8
Create Date: 2026-11-22 12:00:00.000000
"""

import sqlalchemy as sa

from alembic import op

revision = "c0e2f4a6b8d9"
down_revision = "b9d1e3f5a7c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TYPE shipmentdocumentstatus ADD VALUE IF NOT EXISTS 'imported'"
        )
    op.add_column(
        "shipmentdocument",
        sa.Column("source_diadoc_document_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_shipmentdocument_source_diadoc_document",
        "shipmentdocument",
        "diadocoutgoingdocument",
        ["source_diadoc_document_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_unique_constraint(
        "uq_shipmentdocument_source_diadoc_document",
        "shipmentdocument",
        ["source_diadoc_document_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_shipmentdocument_source_diadoc_document", "shipmentdocument", type_="unique"
    )
    op.drop_constraint(
        "fk_shipmentdocument_source_diadoc_document", "shipmentdocument", type_="foreignkey"
    )
    op.drop_column("shipmentdocument", "source_diadoc_document_id")
