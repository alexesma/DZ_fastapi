"""Marking codes scanned while picking a shipment.

Revision ID: 9c1e3a5b7d46
Revises: 8b0d2f4a6c35
Create Date: 2026-11-13 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "9c1e3a5b7d46"
down_revision: Union[str, Sequence[str], None] = "8b0d2f4a6c35"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "shipmentmarkingscan",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "shipment_document_id",
            sa.Integer(),
            sa.ForeignKey("shipmentdocument.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "shipment_document_item_id",
            sa.Integer(),
            sa.ForeignKey("shipmentdocumentitem.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "marking_code_id",
            sa.Integer(),
            sa.ForeignKey("productmarkingcode.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("code", sa.String(length=255), nullable=False),
        sa.Column("scanned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "scanned_by_user_id",
            sa.Integer(),
            sa.ForeignKey("app_user.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.UniqueConstraint(
            "shipment_document_id",
            "marking_code_id",
            name="uq_shipment_marking_scan",
        ),
    )
    op.create_index(
        "ix_shipmentmarkingscan_shipment_document_id",
        "shipmentmarkingscan",
        ["shipment_document_id"],
    )
    op.create_index(
        "ix_shipmentmarkingscan_marking_code_id",
        "shipmentmarkingscan",
        ["marking_code_id"],
    )
    # По умолчанию поведение прежнее: коды назначаются автоматически.
    op.add_column(
        "gismtsettings",
        sa.Column(
            "shipment_codes_auto_fifo",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )


def downgrade() -> None:
    op.drop_column("gismtsettings", "shipment_codes_auto_fifo")
    op.drop_index(
        "ix_shipmentmarkingscan_marking_code_id",
        table_name="shipmentmarkingscan",
    )
    op.drop_index(
        "ix_shipmentmarkingscan_shipment_document_id",
        table_name="shipmentmarkingscan",
    )
    op.drop_table("shipmentmarkingscan")
