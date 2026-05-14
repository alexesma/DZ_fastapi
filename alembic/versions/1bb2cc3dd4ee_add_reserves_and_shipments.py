"""Add StockReserve and ShipmentDocument tables

Revision ID: 1bb2cc3dd4ee
Revises: 0aa1bb2cc3dd, 6c7d8e9f0a1b
Create Date: 2026-05-04 00:01:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '1bb2cc3dd4ee'
down_revision: Union[str, Sequence[str], None] = (
    '0aa1bb2cc3dd',
    '6c7d8e9f0a1b',
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── новые enum-ы ────────────────────────────────────────────────────────
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE reservestatus AS ENUM (
                'active', 'released', 'cancelled', 'expired'
            );
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE shipmentdocumentstatus AS ENUM (
                'draft', 'posted', 'cancelled'
            );
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )

    # ── stockreserve ────────────────────────────────────────────────────────
    op.create_table(
        'stockreserve',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('storage_location_id', sa.Integer(), nullable=True),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column(
            'status',
            postgresql.ENUM(
                'active', 'released', 'cancelled', 'expired',
                name='reservestatus', create_type=False,
            ),
            nullable=False,
            server_default='active',
        ),
        sa.Column('customer_order_item_id', sa.Integer(), nullable=True),
        sa.Column('stock_order_item_id', sa.Integer(), nullable=True),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('released_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(100), nullable=True),
        sa.Column(
            'sync_status',
            postgresql.ENUM(
                'pending', 'synced', 'error',
                name='syncstatus', create_type=False,
            ),
            nullable=False,
            server_default='pending',
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.ForeignKeyConstraint(['autopart_id'], ['autopart.id'],
                                ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['storage_location_id'], ['storagelocation.id'],
                                ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['customer_order_item_id'],
                                ['customerorderitem.id'],
                                ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['stock_order_item_id'],
                                ['stockorderitem.id'],
                                ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_stockreserve_autopart_id',
                    'stockreserve', ['autopart_id'])
    op.create_index('ix_stockreserve_storage_location_id',
                    'stockreserve', ['storage_location_id'])
    op.create_index('ix_stockreserve_status',
                    'stockreserve', ['status'])
    op.create_index('ix_stockreserve_customer_order_item_id',
                    'stockreserve', ['customer_order_item_id'])
    op.create_index('ix_stockreserve_stock_order_item_id',
                    'stockreserve', ['stock_order_item_id'])
    op.create_index('ix_stockreserve_external_id',
                    'stockreserve', ['external_id'])
    op.create_index('ix_stockreserve_sync_status',
                    'stockreserve', ['sync_status'])
    op.create_index('idx_stockreserve_active',
                    'stockreserve',
                    ['autopart_id', 'storage_location_id', 'status'])

    # ── shipmentdocument ────────────────────────────────────────────────────
    op.create_table(
        'shipmentdocument',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('doc_number', sa.String(100), nullable=True),
        sa.Column('doc_date', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column(
            'status',
            postgresql.ENUM(
                'draft', 'posted', 'cancelled',
                name='shipmentdocumentstatus', create_type=False,
            ),
            nullable=False,
            server_default='draft',
        ),
        sa.Column('customer_id', sa.Integer(), nullable=True),
        sa.Column('customer_order_id', sa.Integer(), nullable=True),
        sa.Column('warehouse_id', sa.Integer(), nullable=True),
        sa.Column('reason', sa.String(255), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(100), nullable=True),
        sa.Column(
            'sync_status',
            postgresql.ENUM(
                'pending', 'synced', 'error',
                name='syncstatus', create_type=False,
            ),
            nullable=False,
            server_default='pending',
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("NOW()")),
        sa.Column('posted_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['customer_id'], ['customer.id'],
                                ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['customer_order_id'], ['customerorder.id'],
                                ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['warehouse_id'], ['warehouse.id'],
                                ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_shipmentdocument_doc_number',
                    'shipmentdocument', ['doc_number'])
    op.create_index('ix_shipmentdocument_status',
                    'shipmentdocument', ['status'])
    op.create_index('ix_shipmentdocument_customer_id',
                    'shipmentdocument', ['customer_id'])
    op.create_index('ix_shipmentdocument_customer_order_id',
                    'shipmentdocument', ['customer_order_id'])
    op.create_index('ix_shipmentdocument_warehouse_id',
                    'shipmentdocument', ['warehouse_id'])
    op.create_index('ix_shipmentdocument_external_id',
                    'shipmentdocument', ['external_id'])
    op.create_index('ix_shipmentdocument_sync_status',
                    'shipmentdocument', ['sync_status'])

    # ── shipmentdocumentitem ────────────────────────────────────────────────
    op.create_table(
        'shipmentdocumentitem',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('document_id', sa.Integer(), nullable=False),
        sa.Column('autopart_id', sa.Integer(), nullable=False),
        sa.Column('storage_location_id', sa.Integer(), nullable=True),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('price', sa.DECIMAL(precision=10, scale=2), nullable=True),
        sa.Column('reserve_id', sa.Integer(), nullable=True),
        sa.Column('lot_id', sa.Integer(), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['document_id'], ['shipmentdocument.id'],
                                ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['autopart_id'], ['autopart.id'],
                                ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['storage_location_id'], ['storagelocation.id'],
                                ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['reserve_id'], ['stockreserve.id'],
                                ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['lot_id'], ['stocklot.id'],
                                ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_shipmentdocumentitem_document_id',
                    'shipmentdocumentitem', ['document_id'])
    op.create_index('ix_shipmentdocumentitem_reserve_id',
                    'shipmentdocumentitem', ['reserve_id'])


def downgrade() -> None:
    op.drop_table('shipmentdocumentitem')
    op.drop_index('ix_shipmentdocument_sync_status',
                  table_name='shipmentdocument')
    op.drop_index('ix_shipmentdocument_external_id',
                  table_name='shipmentdocument')
    op.drop_index('ix_shipmentdocument_warehouse_id',
                  table_name='shipmentdocument')
    op.drop_index('ix_shipmentdocument_customer_order_id',
                  table_name='shipmentdocument')
    op.drop_index('ix_shipmentdocument_customer_id',
                  table_name='shipmentdocument')
    op.drop_index('ix_shipmentdocument_status',
                  table_name='shipmentdocument')
    op.drop_index('ix_shipmentdocument_doc_number',
                  table_name='shipmentdocument')
    op.drop_table('shipmentdocument')

    op.drop_index('idx_stockreserve_active', table_name='stockreserve')
    op.drop_index('ix_stockreserve_sync_status', table_name='stockreserve')
    op.drop_index('ix_stockreserve_external_id', table_name='stockreserve')
    op.drop_index('ix_stockreserve_stock_order_item_id',
                  table_name='stockreserve')
    op.drop_index('ix_stockreserve_customer_order_item_id',
                  table_name='stockreserve')
    op.drop_index('ix_stockreserve_status', table_name='stockreserve')
    op.drop_index('ix_stockreserve_storage_location_id',
                  table_name='stockreserve')
    op.drop_index('ix_stockreserve_autopart_id', table_name='stockreserve')
    op.drop_table('stockreserve')

    op.execute("DROP TYPE IF EXISTS shipmentdocumentstatus")
    op.execute("DROP TYPE IF EXISTS reservestatus")
