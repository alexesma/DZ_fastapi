"""Add StockLot for GTD/batch tracking (FIFO)

Revision ID: a9b0c1d2e3f4
Revises: e3f4a5b6c7d8
Create Date: 2026-04-30 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a9b0c1d2e3f4'
down_revision: Union[str, None] = 'e3f4a5b6c7d8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- stocklot table ---
    op.create_table(
        'stocklot',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('storage_location_id', sa.Integer(), nullable=True),
        sa.Column('gtd_number', sa.String(64), nullable=True),
        sa.Column('country_code', sa.String(16), nullable=True),
        sa.Column('country_name', sa.String(120), nullable=True),
        sa.Column('initial_quantity', sa.Integer(), nullable=False),
        sa.Column('remaining_quantity', sa.Integer(), nullable=False),
        sa.Column('source_receipt_id', sa.Integer(), nullable=True),
        sa.Column('source_receipt_item_id', sa.Integer(), nullable=True),
        sa.Column(
            'received_at',
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['autopart_id'], ['autopart.id'], ondelete='CASCADE'
        ),
        sa.ForeignKeyConstraint(
            ['storage_location_id'], ['storagelocation.id'],
            ondelete='SET NULL',
        ),
        sa.ForeignKeyConstraint(
            ['source_receipt_id'], ['supplierreceipt.id'],
            ondelete='SET NULL',
        ),
        sa.ForeignKeyConstraint(
            ['source_receipt_item_id'], ['supplierreceiptitem.id'],
            ondelete='SET NULL',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_stocklot_autopart_id', 'stocklot', ['autopart_id'])
    op.create_index(
        'ix_stocklot_storage_location_id', 'stocklot', ['storage_location_id']
    )
    op.create_index('ix_stocklot_gtd_number', 'stocklot', ['gtd_number'])
    op.create_index('ix_stocklot_source_receipt_id', 'stocklot', ['source_receipt_id'])
    op.create_index('ix_stocklot_received_at', 'stocklot', ['received_at'])
    op.create_index(
        'idx_stocklot_fifo',
        'stocklot',
        ['autopart_id', 'storage_location_id', 'remaining_quantity', 'received_at'],
    )

    # --- stock_lot_id on stockmovement ---
    op.add_column(
        'stockmovement',
        sa.Column('stock_lot_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_stockmovement_stocklot',
        'stockmovement', 'stocklot',
        ['stock_lot_id'], ['id'],
        ondelete='SET NULL',
    )
    op.create_index(
        'ix_stockmovement_stock_lot_id', 'stockmovement', ['stock_lot_id']
    )


def downgrade() -> None:
    op.drop_index('ix_stockmovement_stock_lot_id', table_name='stockmovement')
    op.drop_constraint(
        'fk_stockmovement_stocklot', 'stockmovement', type_='foreignkey'
    )
    op.drop_column('stockmovement', 'stock_lot_id')

    op.drop_index('idx_stocklot_fifo', table_name='stocklot')
    op.drop_index('ix_stocklot_received_at', table_name='stocklot')
    op.drop_index('ix_stocklot_source_receipt_id', table_name='stocklot')
    op.drop_index('ix_stocklot_gtd_number', table_name='stocklot')
    op.drop_index('ix_stocklot_storage_location_id', table_name='stocklot')
    op.drop_index('ix_stocklot_autopart_id', table_name='stocklot')
    op.drop_table('stocklot')
