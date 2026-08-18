"""link stock orders to shipment documents

Revision ID: c7e9a4b2d610
Revises: af5b8d2e4c71
Create Date: 2026-08-10 14:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c7e9a4b2d610"
down_revision: Union[str, Sequence[str], None] = "af5b8d2e4c71"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "stockorder",
        sa.Column("shipment_document_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_stockorder_shipment_document",
        "stockorder",
        "shipmentdocument",
        ["shipment_document_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_stockorder_shipment_document_id",
        "stockorder",
        ["shipment_document_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_stockorder_shipment_document_id",
        table_name="stockorder",
    )
    op.drop_constraint(
        "fk_stockorder_shipment_document",
        "stockorder",
        type_="foreignkey",
    )
    op.drop_column("stockorder", "shipment_document_id")
