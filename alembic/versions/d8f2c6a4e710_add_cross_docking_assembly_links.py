"""add cross-docking assembly links

Revision ID: d8f2c6a4e710
Revises: c7e9a4b2d610
Create Date: 2026-08-10 19:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d8f2c6a4e710"
down_revision: Union[str, Sequence[str], None] = "c7e9a4b2d610"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "stockorderitem",
        sa.Column("supplier_receipt_item_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "stockorderitem",
        sa.Column("preferred_stock_lot_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_stockorderitem_supplier_receipt_item",
        "stockorderitem",
        "supplierreceiptitem",
        ["supplier_receipt_item_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_stockorderitem_preferred_stock_lot",
        "stockorderitem",
        "stocklot",
        ["preferred_stock_lot_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_stockorderitem_supplier_receipt_item_id",
        "stockorderitem",
        ["supplier_receipt_item_id"],
        unique=True,
    )
    op.create_index(
        "ix_stockorderitem_preferred_stock_lot_id",
        "stockorderitem",
        ["preferred_stock_lot_id"],
        unique=False,
    )

    op.add_column(
        "shipmentdocumentitem",
        sa.Column("preferred_lot_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_shipmentdocumentitem_preferred_lot",
        "shipmentdocumentitem",
        "stocklot",
        ["preferred_lot_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_shipmentdocumentitem_preferred_lot_id",
        "shipmentdocumentitem",
        ["preferred_lot_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_shipmentdocumentitem_preferred_lot_id",
        table_name="shipmentdocumentitem",
    )
    op.drop_constraint(
        "fk_shipmentdocumentitem_preferred_lot",
        "shipmentdocumentitem",
        type_="foreignkey",
    )
    op.drop_column("shipmentdocumentitem", "preferred_lot_id")

    op.drop_index(
        "ix_stockorderitem_preferred_stock_lot_id",
        table_name="stockorderitem",
    )
    op.drop_index(
        "ix_stockorderitem_supplier_receipt_item_id",
        table_name="stockorderitem",
    )
    op.drop_constraint(
        "fk_stockorderitem_preferred_stock_lot",
        "stockorderitem",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_stockorderitem_supplier_receipt_item",
        "stockorderitem",
        type_="foreignkey",
    )
    op.drop_column("stockorderitem", "preferred_stock_lot_id")
    op.drop_column("stockorderitem", "supplier_receipt_item_id")
