"""add inventory costing and shipment profit allocations

Revision ID: 5ab1c2d3e4f5
Revises: 4ee5ff6aa7bb
Create Date: 2026-05-16 13:10:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = '5ab1c2d3e4f5'
down_revision: Union[str, Sequence[str], None] = '4ee5ff6aa7bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'stocklot',
        sa.Column(
            'cost_price',
            sa.Numeric(12, 4),
            nullable=True,
            comment='Закупочная/учётная себестоимость одной единицы партии',
        ),
    )
    op.add_column(
        'shipmentdocumentitem',
        sa.Column(
            'cost_price',
            sa.Numeric(12, 4),
            nullable=True,
            comment='Снимок себестоимости за единицу на момент проведения',
        ),
    )
    op.add_column(
        'shipmentdocumentitem',
        sa.Column(
            'cost_total',
            sa.Numeric(14, 2),
            nullable=True,
            comment='Суммарная себестоимость строки на момент проведения',
        ),
    )
    op.add_column(
        'stockdocumentitem',
        sa.Column(
            'cost_price',
            sa.Numeric(12, 4),
            nullable=True,
            comment='Себестоимость единицы для ручного оприходования/корректировки',
        ),
    )

    op.create_table(
        'shipmentdocumentitemlotallocation',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('shipment_document_item_id', sa.Integer(), nullable=False),
        sa.Column('stock_lot_id', sa.Integer(), nullable=True),
        sa.Column('stock_movement_id', sa.Integer(), nullable=True),
        sa.Column('provider_id', sa.Integer(), nullable=True),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('unit_cost_price', sa.Numeric(12, 4), nullable=True),
        sa.Column('total_cost_price', sa.Numeric(14, 2), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ['provider_id'],
            ['provider.id'],
            ondelete='SET NULL',
        ),
        sa.ForeignKeyConstraint(
            ['shipment_document_item_id'],
            ['shipmentdocumentitem.id'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['stock_lot_id'],
            ['stocklot.id'],
            ondelete='SET NULL',
        ),
        sa.ForeignKeyConstraint(
            ['stock_movement_id'],
            ['stockmovement.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_shipmentdocumentitemlotallocation_provider_id'),
        'shipmentdocumentitemlotallocation',
        ['provider_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_shipmentdocumentitemlotallocation_shipment_document_item_id'),
        'shipmentdocumentitemlotallocation',
        ['shipment_document_item_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_shipmentdocumentitemlotallocation_stock_lot_id'),
        'shipmentdocumentitemlotallocation',
        ['stock_lot_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_shipmentdocumentitemlotallocation_stock_movement_id'),
        'shipmentdocumentitemlotallocation',
        ['stock_movement_id'],
        unique=True,
    )
    op.create_index(
        'idx_shipment_item_lot_alloc_report',
        'shipmentdocumentitemlotallocation',
        ['provider_id', 'shipment_document_item_id', 'stock_lot_id'],
        unique=False,
    )

    op.execute(
        """
        UPDATE stocklot AS sl
        SET cost_price = COALESCE(
            sri.price,
            CASE
                WHEN COALESCE(sri.received_quantity, 0) > 0
                    AND sri.total_price_with_vat IS NOT NULL
                THEN sri.total_price_with_vat / sri.received_quantity
                ELSE NULL
            END
        )
        FROM supplierreceiptitem AS sri
        WHERE sl.source_receipt_item_id = sri.id
          AND sl.cost_price IS NULL
        """
    )


def downgrade() -> None:
    op.drop_index(
        'idx_shipment_item_lot_alloc_report',
        table_name='shipmentdocumentitemlotallocation',
    )
    op.drop_index(
        op.f('ix_shipmentdocumentitemlotallocation_stock_movement_id'),
        table_name='shipmentdocumentitemlotallocation',
    )
    op.drop_index(
        op.f('ix_shipmentdocumentitemlotallocation_stock_lot_id'),
        table_name='shipmentdocumentitemlotallocation',
    )
    op.drop_index(
        op.f('ix_shipmentdocumentitemlotallocation_shipment_document_item_id'),
        table_name='shipmentdocumentitemlotallocation',
    )
    op.drop_index(
        op.f('ix_shipmentdocumentitemlotallocation_provider_id'),
        table_name='shipmentdocumentitemlotallocation',
    )
    op.drop_table('shipmentdocumentitemlotallocation')
    op.drop_column('stockdocumentitem', 'cost_price')
    op.drop_column('shipmentdocumentitem', 'cost_total')
    op.drop_column('shipmentdocumentitem', 'cost_price')
    op.drop_column('stocklot', 'cost_price')
