"""add customer order stats indexes

Revision ID: f0a1b2c3d4e5
Revises: e9f0a1b2c3d4
Create Date: 2026-04-01 16:10:00.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f0a1b2c3d4e5'
down_revision: Union[str, None] = 'e9f0a1b2c3d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'ix_customerorder_customer_id_received_at',
        'customerorder',
        ['customer_id', 'received_at'],
        unique=False,
    )
    op.create_index(
        'ix_customerorderitem_order_id',
        'customerorderitem',
        ['order_id'],
        unique=False,
    )
    op.create_index(
        'ix_customerorderitem_oem',
        'customerorderitem',
        ['oem'],
        unique=False,
    )
    op.create_index(
        'ix_customerorderitem_brand',
        'customerorderitem',
        ['brand'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_customerorderitem_brand', table_name='customerorderitem')
    op.drop_index('ix_customerorderitem_oem', table_name='customerorderitem')
    op.drop_index(
        'ix_customerorderitem_order_id',
        table_name='customerorderitem',
    )
    op.drop_index(
        'ix_customerorder_customer_id_received_at',
        table_name='customerorder',
    )
