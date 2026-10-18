"""Track supplier item decision provenance and document grace periods.

Revision ID: f8c0d2e4a316
Revises: e7b9c1d3f205
"""

from alembic import op
import sqlalchemy as sa


revision = "f8c0d2e4a316"
down_revision = "e7b9c1d3f205"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "supplierorderitem", sa.Column("decision_source", sa.String(32), nullable=True)
    )
    op.add_column(
        "supplierorderitem",
        sa.Column("document_missing_since", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "supplierorderitem",
        sa.Column("document_missing_message_id", sa.Integer(), nullable=True),
    )
    op.create_index(
        "ix_supplierorderitem_decision_source", "supplierorderitem", ["decision_source"]
    )
    op.create_index(
        "ix_supplierorderitem_document_missing_since",
        "supplierorderitem", ["document_missing_since"],
    )
    op.create_foreign_key(
        "fk_supplierorderitem_document_missing_message",
        "supplierorderitem", "supplierordermessage",
        ["document_missing_message_id"], ["id"],
    )


def downgrade():
    op.drop_constraint(
        "fk_supplierorderitem_document_missing_message", "supplierorderitem",
        type_="foreignkey",
    )
    op.drop_index("ix_supplierorderitem_document_missing_since", "supplierorderitem")
    op.drop_index("ix_supplierorderitem_decision_source", "supplierorderitem")
    op.drop_column("supplierorderitem", "document_missing_message_id")
    op.drop_column("supplierorderitem", "document_missing_since")
    op.drop_column("supplierorderitem", "decision_source")
