"""add_customer_orders

Revision ID: 1c8b9d2a4e01
Revises: f9a1d2b3c4d5
Create Date: 2026-02-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1c8b9d2a4e01'
down_revision = 'f9a1d2b3c4d5'
branch_labels = None
depends_on = None


customer_order_status = sa.Enum(
    'NEW',
    'PROCESSED',
    'SENT',
    'ERROR',
    name='customerorderstatus',
)
customer_order_item_status = sa.Enum(
    'NEW',
    'OWN_STOCK',
    'SUPPLIER',
    'REJECTED',
    name='customerorderitemstatus',
)
supplier_order_status = sa.Enum(
    'NEW',
    'SCHEDULED',
    'SENT',
    'ERROR',
    name='supplierorderstatus',
)
stock_order_status = sa.Enum(
    'NEW',
    'COMPLETED',
    'ERROR',
    name='stockorderstatus',
)
customer_order_ship_mode = sa.Enum(
    'REPLACE_QTY',
    'WRITE_SHIP_QTY',
    'WRITE_REJECT_QTY',
    name='customerordershipmode',
)

customer_order_status_ct = postgresql.ENUM(
    'NEW',
    'PROCESSED',
    'SENT',
    'ERROR',
    name='customerorderstatus',
    create_type=False,
)
customer_order_item_status_ct = postgresql.ENUM(
    'NEW',
    'OWN_STOCK',
    'SUPPLIER',
    'REJECTED',
    name='customerorderitemstatus',
    create_type=False,
)
supplier_order_status_ct = postgresql.ENUM(
    'NEW',
    'SCHEDULED',
    'SENT',
    'ERROR',
    name='supplierorderstatus',
    create_type=False,
)
stock_order_status_ct = postgresql.ENUM(
    'NEW',
    'COMPLETED',
    'ERROR',
    name='stockorderstatus',
    create_type=False,
)
customer_order_ship_mode_ct = postgresql.ENUM(
    'REPLACE_QTY',
    'WRITE_SHIP_QTY',
    'WRITE_REJECT_QTY',
    name='customerordershipmode',
    create_type=False,
)


def upgrade():
    op.execute(
        """DO $$
BEGIN
    CREATE TYPE customerorderstatus AS ENUM ('NEW','PROCESSED','SENT','ERROR');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;"""
    )
    op.execute(
        """DO $$
BEGIN
    CREATE TYPE customerorderitemstatus AS ENUM ('NEW','OWN_STOCK','SUPPLIER','REJECTED');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;"""
    )
    op.execute(
        """DO $$
BEGIN
    CREATE TYPE supplierorderstatus AS ENUM ('NEW','SCHEDULED','SENT','ERROR');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;"""
    )
    op.execute(
        """DO $$
BEGIN
    CREATE TYPE stockorderstatus AS ENUM ('NEW','COMPLETED','ERROR');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;"""
    )
    op.execute(
        """DO $$
BEGIN
    CREATE TYPE customerordershipmode AS ENUM ('REPLACE_QTY','WRITE_SHIP_QTY','WRITE_REJECT_QTY');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;"""
    )

    op.add_column(
        'provider',
        sa.Column('order_schedule_days', sa.JSON(), server_default='[]'),
    )
    op.add_column(
        'provider',
        sa.Column('order_schedule_times', sa.JSON(), server_default='[]'),
    )
    op.add_column(
        'provider',
        sa.Column(
            'order_schedule_enabled', sa.Boolean(), server_default='false'
        ),
    )

    op.create_table(
        'emailaccount',
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('password', sa.String(length=255), nullable=False),
        sa.Column('imap_host', sa.String(length=255), nullable=True),
        sa.Column('imap_port', sa.Integer(), nullable=True),
        sa.Column('smtp_host', sa.String(length=255), nullable=True),
        sa.Column('smtp_port', sa.Integer(), nullable=True),
        sa.Column('smtp_use_ssl', sa.Boolean(), nullable=True),
        sa.Column('purposes', sa.JSON(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('id', sa.Integer(), primary_key=True),
    )
    op.create_index('ix_emailaccount_email', 'emailaccount', ['email'], unique=True)

    op.create_table(
        'customerorderconfig',
        sa.Column('customer_id', sa.Integer(), nullable=False),
        sa.Column('order_email', sa.String(length=255), nullable=True),
        sa.Column('order_emails', sa.JSON(), server_default='[]'),
        sa.Column('order_subject_pattern', sa.String(length=255), nullable=True),
        sa.Column('order_filename_pattern', sa.String(length=255), nullable=True),
        sa.Column('order_reply_emails', sa.JSON(), server_default='[]'),
        sa.Column('pricelist_config_id', sa.Integer(), nullable=True),
        sa.Column('order_number_column', sa.Integer(), nullable=True),
        sa.Column('order_date_column', sa.Integer(), nullable=True),
        sa.Column('order_number_regex_subject', sa.String(length=255), nullable=True),
        sa.Column('order_number_regex_filename', sa.String(length=255), nullable=True),
        sa.Column('order_number_regex_body', sa.String(length=255), nullable=True),
        sa.Column('order_number_prefix', sa.String(length=255), nullable=True),
        sa.Column('order_number_suffix', sa.String(length=255), nullable=True),
        sa.Column('order_number_source', sa.String(length=32), nullable=True),
        sa.Column('oem_col', sa.Integer(), nullable=False),
        sa.Column('brand_col', sa.Integer(), nullable=False),
        sa.Column('name_col', sa.Integer(), nullable=True),
        sa.Column('qty_col', sa.Integer(), nullable=False),
        sa.Column('price_col', sa.Integer(), nullable=True),
        sa.Column('ship_qty_col', sa.Integer(), nullable=True),
        sa.Column('reject_qty_col', sa.Integer(), nullable=True),
        sa.Column('ship_mode', customer_order_ship_mode_ct, nullable=False),
        sa.Column('price_tolerance_pct', sa.Float(), server_default='2.0'),
        sa.Column('price_warning_pct', sa.Float(), server_default='5.0'),
        sa.Column('is_active', sa.Boolean(), server_default='true'),
        sa.Column('last_uid', sa.Integer(), server_default='0'),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['customer_id'], ['customer.id']),
        sa.ForeignKeyConstraint(
            ['pricelist_config_id'], ['customerpricelistconfig.id']
        ),
    )
    op.create_index(
        'ix_customerorderconfig_order_email',
        'customerorderconfig',
        ['order_email'],
        unique=False,
    )

    op.create_table(
        'customerorder',
        sa.Column('customer_id', sa.Integer(), nullable=False),
        sa.Column('status', customer_order_status_ct, nullable=False),
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('processed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('source_email', sa.String(length=255), nullable=True),
        sa.Column('source_uid', sa.Integer(), nullable=True),
        sa.Column('source_subject', sa.String(length=255), nullable=True),
        sa.Column('source_filename', sa.String(length=255), nullable=True),
        sa.Column('file_hash', sa.String(length=64), nullable=True),
        sa.Column('order_number', sa.String(length=255), nullable=True),
        sa.Column('order_date', sa.Date(), nullable=True),
        sa.Column('response_file_path', sa.String(length=255), nullable=True),
        sa.Column('response_file_name', sa.String(length=255), nullable=True),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['customer_id'], ['customer.id']),
    )
    op.create_index(
        'ix_customerorder_file_hash',
        'customerorder',
        ['file_hash'],
        unique=False,
    )

    op.create_table(
        'customerorderitem',
        sa.Column('order_id', sa.Integer(), nullable=False),
        sa.Column('row_index', sa.Integer(), nullable=True),
        sa.Column('oem', sa.String(length=255), nullable=False),
        sa.Column('brand', sa.String(length=255), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=True),
        sa.Column('requested_qty', sa.Integer(), nullable=False),
        sa.Column('requested_price', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('ship_qty', sa.Integer(), nullable=True),
        sa.Column('reject_qty', sa.Integer(), nullable=True),
        sa.Column('status', customer_order_item_status_ct, nullable=False),
        sa.Column('supplier_id', sa.Integer(), nullable=True),
        sa.Column('autopart_id', sa.Integer(), nullable=True),
        sa.Column('matched_price', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('price_diff_pct', sa.Float(), nullable=True),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['autopart_id'], ['autopart.id']),
        sa.ForeignKeyConstraint(['order_id'], ['customerorder.id']),
        sa.ForeignKeyConstraint(['supplier_id'], ['provider.id']),
    )

    op.create_table(
        'supplierorder',
        sa.Column('provider_id', sa.Integer(), nullable=False),
        sa.Column('status', supplier_order_status_ct, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('scheduled_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['provider_id'], ['provider.id']),
    )

    op.create_table(
        'supplierorderitem',
        sa.Column('supplier_order_id', sa.Integer(), nullable=False),
        sa.Column('customer_order_item_id', sa.Integer(), nullable=True),
        sa.Column('autopart_id', sa.Integer(), nullable=True),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('price', sa.DECIMAL(10, 2), nullable=True),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['autopart_id'], ['autopart.id']),
        sa.ForeignKeyConstraint(['customer_order_item_id'], ['customerorderitem.id']),
        sa.ForeignKeyConstraint(['supplier_order_id'], ['supplierorder.id']),
    )

    op.create_table(
        'stockorder',
        sa.Column('customer_id', sa.Integer(), nullable=True),
        sa.Column('status', stock_order_status_ct, nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['customer_id'], ['customer.id']),
    )

    op.create_table(
        'stockorderitem',
        sa.Column('stock_order_id', sa.Integer(), nullable=False),
        sa.Column('customer_order_item_id', sa.Integer(), nullable=True),
        sa.Column('autopart_id', sa.Integer(), nullable=True),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.ForeignKeyConstraint(['autopart_id'], ['autopart.id']),
        sa.ForeignKeyConstraint(['customer_order_item_id'], ['customerorderitem.id']),
        sa.ForeignKeyConstraint(['stock_order_id'], ['stockorder.id']),
    )


def downgrade():
    op.drop_table('stockorderitem')
    op.drop_table('stockorder')
    op.drop_table('supplierorderitem')
    op.drop_table('supplierorder')
    op.drop_table('customerorderitem')
    op.drop_index('ix_customerorder_file_hash', table_name='customerorder')
    op.drop_table('customerorder')
    op.drop_index(
        'ix_customerorderconfig_order_email',
        table_name='customerorderconfig',
    )
    op.drop_table('customerorderconfig')

    op.drop_index('ix_emailaccount_email', table_name='emailaccount')
    op.drop_table('emailaccount')

    op.drop_column('provider', 'order_schedule_enabled')
    op.drop_column('provider', 'order_schedule_times')
    op.drop_column('provider', 'order_schedule_days')

    stock_order_status.drop(op.get_bind(), checkfirst=True)
    supplier_order_status.drop(op.get_bind(), checkfirst=True)
    customer_order_item_status.drop(op.get_bind(), checkfirst=True)
    customer_order_status.drop(op.get_bind(), checkfirst=True)
    customer_order_ship_mode.drop(op.get_bind(), checkfirst=True)
