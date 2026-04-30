"""Add warehouse_id to SupplierReceiptItem

Revision ID: c1d2e3f4a5b6
Revises: 6c7d8e9f0a1b
Create Date: 2026-04-30 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c1d2e3f4a5b6'
down_revision: Union[str, None] = '6c7d8e9f0a1b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'supplierreceiptitem',
        sa.Column(
            'warehouse_id',
            sa.Integer(),
            sa.ForeignKey('warehouse.id'),
            nullable=True,
        ),
    )
    op.create_index(
        'ix_supplierreceiptitem_warehouse_id',
        'supplierreceiptitem',
        ['warehouse_id'],
    )


def downgrade() -> None:
    op.drop_index(
        'ix_supplierreceiptitem_warehouse_id',
        table_name='supplierreceiptitem',
    )
    op.drop_column('supplierreceiptitem', 'warehouse_id')
