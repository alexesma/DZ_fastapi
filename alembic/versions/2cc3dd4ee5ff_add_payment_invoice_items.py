"""Add PaymentInvoiceItem and update PaymentInvoice for items

Revision ID: 2cc3dd4ee5ff
Revises: b9b45e996afd
Create Date: 2026-05-12 14:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '2cc3dd4ee5ff'
down_revision: Union[str, None] = 'b9b45e996afd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'paymentinvoiceitem',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('invoice_id', sa.Integer(), nullable=False),
        sa.Column('position', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('autopart_id', sa.Integer(), nullable=True),
        sa.Column('name', sa.String(500), nullable=False),
        sa.Column('oem_number', sa.String(100), nullable=True),
        sa.Column('quantity', sa.DECIMAL(10, 3), nullable=False, server_default='1.000'),
        sa.Column('unit_price', sa.DECIMAL(12, 2), nullable=False, server_default='0.00'),
        sa.Column('vat_rate', sa.DECIMAL(5, 2), nullable=False, server_default='20.00'),
        sa.Column('total', sa.DECIMAL(12, 2), nullable=False, server_default='0.00'),
        sa.ForeignKeyConstraint(['invoice_id'], ['paymentinvoice.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['autopart_id'], ['autopart.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_paymentinvoiceitem_invoice_id', 'paymentinvoiceitem', ['invoice_id'])
    op.create_index('ix_paymentinvoiceitem_autopart_id', 'paymentinvoiceitem', ['autopart_id'])


def downgrade() -> None:
    op.drop_index('ix_paymentinvoiceitem_autopart_id', table_name='paymentinvoiceitem')
    op.drop_index('ix_paymentinvoiceitem_invoice_id', table_name='paymentinvoiceitem')
    op.drop_table('paymentinvoiceitem')
