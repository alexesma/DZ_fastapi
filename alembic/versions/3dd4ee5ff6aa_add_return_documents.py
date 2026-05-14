"""add return documents

Revision ID: 3dd4ee5ff6aa
Revises: b2d4f6a8c0e1, 1bb2cc3dd4ee
Create Date: 2026-05-12 20:40:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = '3dd4ee5ff6aa'
down_revision: Union[str, Sequence[str], None] = (
    'b2d4f6a8c0e1',
    '1bb2cc3dd4ee',
)
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    return_status = postgresql.ENUM(
        'created',
        'approved',
        'shipped',
        'confirmed',
        'rejected',
        name='returndocumentstatus',
        create_type=False,
    )
    sync_status = postgresql.ENUM(
        'pending',
        'synced',
        'error',
        name='syncstatus',
        create_type=False,
    )
    return_status.create(bind, checkfirst=True)

    op.create_table(
        'returnfromcustomer',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_number', sa.String(length=100), nullable=True),
        sa.Column('doc_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            'status',
            return_status,
            nullable=False,
            server_default='created',
        ),
        sa.Column(
            'customer_id',
            sa.Integer(),
            sa.ForeignKey('customer.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'shipment_document_id',
            sa.Integer(),
            sa.ForeignKey('shipmentdocument.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'warehouse_id',
            sa.Integer(),
            sa.ForeignKey('warehouse.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'created_by_user_id',
            sa.Integer(),
            sa.ForeignKey('app_user.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'diadoc_outgoing_document_id',
            sa.Integer(),
            sa.ForeignKey('diadocoutgoingdocument.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column('reason', sa.String(length=255), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(length=100), nullable=True),
        sa.Column(
            'sync_status',
            sync_status,
            nullable=False,
            server_default='pending',
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text('now()'),
        ),
        sa.Column('approved_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('shipped_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('confirmed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('rejected_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        'ix_returnfromcustomer_doc_number',
        'returnfromcustomer',
        ['doc_number'],
    )
    op.create_index(
        'ix_returnfromcustomer_status',
        'returnfromcustomer',
        ['status'],
    )
    op.create_index(
        'ix_returnfromcustomer_customer_id',
        'returnfromcustomer',
        ['customer_id'],
    )
    op.create_index(
        'ix_returnfromcustomer_shipment_document_id',
        'returnfromcustomer',
        ['shipment_document_id'],
    )
    op.create_index(
        'ix_returnfromcustomer_warehouse_id',
        'returnfromcustomer',
        ['warehouse_id'],
    )
    op.create_index(
        'ix_returnfromcustomer_diadoc_outgoing_document_id',
        'returnfromcustomer',
        ['diadoc_outgoing_document_id'],
    )
    op.create_index(
        'ix_returnfromcustomer_external_id',
        'returnfromcustomer',
        ['external_id'],
    )
    op.create_index(
        'ix_returnfromcustomer_sync_status',
        'returnfromcustomer',
        ['sync_status'],
    )

    op.create_table(
        'returntosupplier',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('doc_number', sa.String(length=100), nullable=True),
        sa.Column('doc_date', sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            'status',
            return_status,
            nullable=False,
            server_default='created',
        ),
        sa.Column(
            'provider_id',
            sa.Integer(),
            sa.ForeignKey('provider.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'supplier_receipt_id',
            sa.Integer(),
            sa.ForeignKey('supplierreceipt.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'warehouse_id',
            sa.Integer(),
            sa.ForeignKey('warehouse.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'created_by_user_id',
            sa.Integer(),
            sa.ForeignKey('app_user.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'diadoc_outgoing_document_id',
            sa.Integer(),
            sa.ForeignKey('diadocoutgoingdocument.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column('reason', sa.String(length=255), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(length=100), nullable=True),
        sa.Column(
            'sync_status',
            sync_status,
            nullable=False,
            server_default='pending',
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text('now()'),
        ),
        sa.Column('approved_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('shipped_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('confirmed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('rejected_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        'ix_returntosupplier_doc_number',
        'returntosupplier',
        ['doc_number'],
    )
    op.create_index(
        'ix_returntosupplier_status',
        'returntosupplier',
        ['status'],
    )
    op.create_index(
        'ix_returntosupplier_provider_id',
        'returntosupplier',
        ['provider_id'],
    )
    op.create_index(
        'ix_returntosupplier_supplier_receipt_id',
        'returntosupplier',
        ['supplier_receipt_id'],
    )
    op.create_index(
        'ix_returntosupplier_warehouse_id',
        'returntosupplier',
        ['warehouse_id'],
    )
    op.create_index(
        'ix_returntosupplier_diadoc_outgoing_document_id',
        'returntosupplier',
        ['diadoc_outgoing_document_id'],
    )
    op.create_index(
        'ix_returntosupplier_external_id',
        'returntosupplier',
        ['external_id'],
    )
    op.create_index(
        'ix_returntosupplier_sync_status',
        'returntosupplier',
        ['sync_status'],
    )

    op.create_table(
        'returnitem',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'return_from_customer_id',
            sa.Integer(),
            sa.ForeignKey('returnfromcustomer.id', ondelete='CASCADE'),
            nullable=True,
        ),
        sa.Column(
            'return_to_supplier_id',
            sa.Integer(),
            sa.ForeignKey('returntosupplier.id', ondelete='CASCADE'),
            nullable=True,
        ),
        sa.Column(
            'shipment_item_id',
            sa.Integer(),
            sa.ForeignKey('shipmentdocumentitem.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'supplier_receipt_item_id',
            sa.Integer(),
            sa.ForeignKey('supplierreceiptitem.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'customer_order_item_id',
            sa.Integer(),
            sa.ForeignKey('customerorderitem.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'supplier_order_item_id',
            sa.Integer(),
            sa.ForeignKey('supplierorderitem.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'order_item_id',
            sa.Integer(),
            sa.ForeignKey('orderitem.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'autopart_id',
            sa.Integer(),
            sa.ForeignKey('autopart.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'storage_location_id',
            sa.Integer(),
            sa.ForeignKey('storagelocation.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column(
            'lot_id',
            sa.Integer(),
            sa.ForeignKey('stocklot.id', ondelete='SET NULL'),
            nullable=True,
        ),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('price', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('gtd_number', sa.String(length=64), nullable=True),
        sa.Column('country_code', sa.String(length=16), nullable=True),
        sa.Column('country_name', sa.String(length=120), nullable=True),
        sa.Column('oem_number', sa.String(length=120), nullable=True),
        sa.Column('brand_name', sa.String(length=120), nullable=True),
        sa.Column('autopart_name', sa.String(length=512), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
    )
    for name, columns in (
        ('ix_returnitem_return_from_customer_id', ['return_from_customer_id']),
        ('ix_returnitem_return_to_supplier_id', ['return_to_supplier_id']),
        ('ix_returnitem_shipment_item_id', ['shipment_item_id']),
        ('ix_returnitem_supplier_receipt_item_id', ['supplier_receipt_item_id']),
        ('ix_returnitem_customer_order_item_id', ['customer_order_item_id']),
        ('ix_returnitem_supplier_order_item_id', ['supplier_order_item_id']),
        ('ix_returnitem_order_item_id', ['order_item_id']),
        ('ix_returnitem_autopart_id', ['autopart_id']),
        ('ix_returnitem_oem_number', ['oem_number']),
    ):
        op.create_index(name, 'returnitem', columns)


def downgrade() -> None:
    op.drop_index('ix_returnitem_oem_number', table_name='returnitem')
    op.drop_index('ix_returnitem_autopart_id', table_name='returnitem')
    op.drop_index('ix_returnitem_order_item_id', table_name='returnitem')
    op.drop_index('ix_returnitem_supplier_order_item_id', table_name='returnitem')
    op.drop_index('ix_returnitem_customer_order_item_id', table_name='returnitem')
    op.drop_index('ix_returnitem_supplier_receipt_item_id', table_name='returnitem')
    op.drop_index('ix_returnitem_shipment_item_id', table_name='returnitem')
    op.drop_index('ix_returnitem_return_to_supplier_id', table_name='returnitem')
    op.drop_index('ix_returnitem_return_from_customer_id', table_name='returnitem')
    op.drop_table('returnitem')

    op.drop_index('ix_returntosupplier_sync_status', table_name='returntosupplier')
    op.drop_index('ix_returntosupplier_external_id', table_name='returntosupplier')
    op.drop_index(
        'ix_returntosupplier_diadoc_outgoing_document_id',
        table_name='returntosupplier',
    )
    op.drop_index('ix_returntosupplier_warehouse_id', table_name='returntosupplier')
    op.drop_index(
        'ix_returntosupplier_supplier_receipt_id',
        table_name='returntosupplier',
    )
    op.drop_index('ix_returntosupplier_provider_id', table_name='returntosupplier')
    op.drop_index('ix_returntosupplier_status', table_name='returntosupplier')
    op.drop_index('ix_returntosupplier_doc_number', table_name='returntosupplier')
    op.drop_table('returntosupplier')

    op.drop_index(
        'ix_returnfromcustomer_sync_status',
        table_name='returnfromcustomer',
    )
    op.drop_index(
        'ix_returnfromcustomer_external_id',
        table_name='returnfromcustomer',
    )
    op.drop_index(
        'ix_returnfromcustomer_diadoc_outgoing_document_id',
        table_name='returnfromcustomer',
    )
    op.drop_index(
        'ix_returnfromcustomer_warehouse_id',
        table_name='returnfromcustomer',
    )
    op.drop_index(
        'ix_returnfromcustomer_shipment_document_id',
        table_name='returnfromcustomer',
    )
    op.drop_index(
        'ix_returnfromcustomer_customer_id',
        table_name='returnfromcustomer',
    )
    op.drop_index('ix_returnfromcustomer_status', table_name='returnfromcustomer')
    op.drop_index(
        'ix_returnfromcustomer_doc_number',
        table_name='returnfromcustomer',
    )
    op.drop_table('returnfromcustomer')

    sa.Enum(name='returndocumentstatus').drop(op.get_bind(), checkfirst=True)
