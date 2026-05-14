"""Add financial documents: PaymentInvoice, CustomerPayment, SupplierPayment

Revision ID: 4f7e4ffc217a
Revises: fd57968e0964
Create Date: 2026-05-12 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '4f7e4ffc217a'
down_revision: Union[str, None] = 'fd57968e0964'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = ('1bb2cc3dd4ee',)


def upgrade() -> None:
    # ── Enum types ────────────────────────────────────────────────────────────
    op.execute(
        """
        DO $$
        BEGIN
            CREATE TYPE invoicestatus AS ENUM (
                'draft', 'sent', 'partially_paid', 'paid', 'cancelled', 'overdue'
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
            CREATE TYPE paymentmethod_customer AS ENUM (
                'bank_transfer', 'cash', 'card', 'offset'
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
            CREATE TYPE paymentmethod_supplier AS ENUM (
                'bank_transfer', 'cash', 'card', 'offset'
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
            CREATE TYPE syncstatus_finance_invoice AS ENUM (
                'pending', 'synced', 'error'
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
            CREATE TYPE syncstatus_finance_cpayment AS ENUM (
                'pending', 'synced', 'error'
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
            CREATE TYPE syncstatus_finance_spayment AS ENUM (
                'pending', 'synced', 'error'
            );
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
        """
    )

    # ── PaymentInvoice ────────────────────────────────────────────────────────
    op.create_table(
        'paymentinvoice',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('customer_id', sa.Integer(), nullable=False),
        sa.Column('shipment_id', sa.Integer(), nullable=True),
        sa.Column('customer_order_id', sa.Integer(), nullable=True),
        sa.Column('invoice_number', sa.String(50), nullable=False),
        sa.Column('invoice_date', sa.Date(), nullable=False),
        sa.Column('due_date', sa.Date(), nullable=True),
        sa.Column('total_amount', sa.DECIMAL(12, 2), nullable=False, server_default='0'),
        sa.Column('paid_amount', sa.DECIMAL(12, 2), nullable=False, server_default='0'),
        sa.Column(
            'status',
            postgresql.ENUM(
                'draft', 'sent', 'partially_paid', 'paid', 'cancelled', 'overdue',
                name='invoicestatus',
                create_type=False,
            ),
            nullable=False,
            server_default='draft',
        ),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(100), nullable=True),
        sa.Column(
            'sync_status',
            postgresql.ENUM(
                'pending', 'synced', 'error',
                name='syncstatus_finance_invoice',
                create_type=False,
            ),
            nullable=False,
            server_default='pending',
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['customer_id'], ['customer.id'], ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(['shipment_id'], ['shipmentdocument.id'], ondelete='SET NULL'),
        sa.ForeignKeyConstraint(['customer_order_id'], ['customerorder.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('invoice_number'),
    )
    op.create_index('ix_paymentinvoice_customer_id', 'paymentinvoice', ['customer_id'])
    op.create_index('ix_paymentinvoice_shipment_id', 'paymentinvoice', ['shipment_id'])
    op.create_index('ix_paymentinvoice_customer_order_id', 'paymentinvoice', ['customer_order_id'])
    op.create_index('ix_paymentinvoice_invoice_number', 'paymentinvoice', ['invoice_number'], unique=True)
    op.create_index('ix_paymentinvoice_external_id', 'paymentinvoice', ['external_id'])

    # ── CustomerPayment ───────────────────────────────────────────────────────
    op.create_table(
        'customerpayment',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('customer_id', sa.Integer(), nullable=False),
        sa.Column('invoice_id', sa.Integer(), nullable=True),
        sa.Column('amount', sa.DECIMAL(12, 2), nullable=False),
        sa.Column('payment_date', sa.Date(), nullable=False),
        sa.Column(
            'payment_method',
            postgresql.ENUM(
                'bank_transfer', 'cash', 'card', 'offset',
                name='paymentmethod_customer',
                create_type=False,
            ),
            nullable=False,
            server_default='bank_transfer',
        ),
        sa.Column('reference', sa.String(255), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(100), nullable=True),
        sa.Column(
            'sync_status',
            postgresql.ENUM(
                'pending', 'synced', 'error',
                name='syncstatus_finance_cpayment',
                create_type=False,
            ),
            nullable=False,
            server_default='pending',
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['customer_id'], ['customer.id'], ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(['invoice_id'], ['paymentinvoice.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_customerpayment_customer_id', 'customerpayment', ['customer_id'])
    op.create_index('ix_customerpayment_invoice_id', 'customerpayment', ['invoice_id'])
    op.create_index('ix_customerpayment_external_id', 'customerpayment', ['external_id'])

    # ── SupplierPayment ───────────────────────────────────────────────────────
    op.create_table(
        'supplierpayment',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('provider_id', sa.Integer(), nullable=False),
        sa.Column('supplier_order_id', sa.Integer(), nullable=True),
        sa.Column('amount', sa.DECIMAL(12, 2), nullable=False),
        sa.Column('payment_date', sa.Date(), nullable=False),
        sa.Column(
            'payment_method',
            postgresql.ENUM(
                'bank_transfer', 'cash', 'card', 'offset',
                name='paymentmethod_supplier',
                create_type=False,
            ),
            nullable=False,
            server_default='bank_transfer',
        ),
        sa.Column('reference', sa.String(255), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('external_id', sa.String(100), nullable=True),
        sa.Column(
            'sync_status',
            postgresql.ENUM(
                'pending', 'synced', 'error',
                name='syncstatus_finance_spayment',
                create_type=False,
            ),
            nullable=False,
            server_default='pending',
        ),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['provider_id'], ['provider.id'], ondelete='RESTRICT'),
        sa.ForeignKeyConstraint(['supplier_order_id'], ['supplierorder.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_supplierpayment_provider_id', 'supplierpayment', ['provider_id'])
    op.create_index('ix_supplierpayment_supplier_order_id', 'supplierpayment', ['supplier_order_id'])
    op.create_index('ix_supplierpayment_external_id', 'supplierpayment', ['external_id'])

    # ── New columns on Customer ───────────────────────────────────────────────
    op.add_column('customer', sa.Column('credit_limit', sa.DECIMAL(12, 2), nullable=True))
    op.add_column('customer', sa.Column('payment_terms_days', sa.Integer(), nullable=False, server_default='0'))

    # ── New columns on Provider ───────────────────────────────────────────────
    op.add_column('provider', sa.Column('payment_terms_days', sa.Integer(), nullable=False, server_default='0'))


def downgrade() -> None:
    # Drop new columns
    op.drop_column('provider', 'payment_terms_days')
    op.drop_column('customer', 'payment_terms_days')
    op.drop_column('customer', 'credit_limit')

    # Drop tables
    op.drop_table('supplierpayment')
    op.drop_table('customerpayment')
    op.drop_table('paymentinvoice')

    # Drop enum types
    op.execute('DROP TYPE IF EXISTS syncstatus_finance_spayment')
    op.execute('DROP TYPE IF EXISTS syncstatus_finance_cpayment')
    op.execute('DROP TYPE IF EXISTS syncstatus_finance_invoice')
    op.execute('DROP TYPE IF EXISTS paymentmethod_supplier')
    op.execute('DROP TYPE IF EXISTS paymentmethod_customer')
    op.execute('DROP TYPE IF EXISTS invoicestatus')
