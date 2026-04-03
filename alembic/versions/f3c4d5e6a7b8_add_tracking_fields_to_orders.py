"""add tracking fields to orders

Revision ID: f3c4d5e6a7b8
Revises: f0a1b2c3d4e5
Create Date: 2026-04-03 14:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f3c4d5e6a7b8'
down_revision: Union[str, None] = 'f0a1b2c3d4e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'supplierorder',
        sa.Column(
            'source_type',
            sa.String(length=32),
            nullable=False,
            server_default='CUSTOMER_ORDER',
        ),
    )
    op.add_column(
        'supplierorder',
        sa.Column('created_by_user_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_supplierorder_created_by_user',
        'supplierorder',
        'app_user',
        ['created_by_user_id'],
        ['id'],
        ondelete='SET NULL',
    )
    op.create_index(
        'ix_supplierorder_created_at', 'supplierorder', ['created_at']
    )
    op.create_index(
        'ix_supplierorder_source_type', 'supplierorder', ['source_type']
    )

    op.add_column(
        'supplierorderitem',
        sa.Column('oem_number', sa.String(length=120), nullable=True),
    )
    op.add_column(
        'supplierorderitem',
        sa.Column('brand_name', sa.String(length=120), nullable=True),
    )
    op.add_column(
        'supplierorderitem',
        sa.Column('autopart_name', sa.String(length=512), nullable=True),
    )
    op.add_column(
        'supplierorderitem',
        sa.Column('min_delivery_day', sa.Integer(), nullable=True),
    )
    op.add_column(
        'supplierorderitem',
        sa.Column('max_delivery_day', sa.Integer(), nullable=True),
    )
    op.add_column(
        'supplierorderitem',
        sa.Column('received_quantity', sa.Integer(), nullable=True),
    )
    op.add_column(
        'supplierorderitem',
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        'ix_supplierorderitem_oem_number',
        'supplierorderitem',
        ['oem_number'],
    )
    op.create_index(
        'ix_supplierorderitem_autopart_id',
        'supplierorderitem',
        ['autopart_id'],
    )

    op.add_column(
        'order',
        sa.Column(
            'source_type',
            sa.String(length=32),
            nullable=False,
            server_default='DRAGONZAP_SEARCH',
        ),
    )
    op.add_column(
        'order',
        sa.Column('created_by_user_id', sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        'fk_order_created_by_user',
        'order',
        'app_user',
        ['created_by_user_id'],
        ['id'],
        ondelete='SET NULL',
    )
    op.create_index('ix_order_created_at', 'order', ['created_at'])
    op.create_index('ix_order_source_type', 'order', ['source_type'])

    op.add_column(
        'orderitem',
        sa.Column('oem_number', sa.String(length=120), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column('brand_name', sa.String(length=120), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column('autopart_name', sa.String(length=512), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column('min_delivery_day', sa.Integer(), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column('max_delivery_day', sa.Integer(), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column('received_quantity', sa.Integer(), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column('received_at', sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index('ix_orderitem_oem_number', 'orderitem', ['oem_number'])
    op.create_index(
        'ix_orderitem_autopart_id', 'orderitem', ['autopart_id']
    )

    op.execute(
        """
        UPDATE supplierorder so
        SET source_type = CASE
            WHEN EXISTS (
                SELECT 1
                FROM supplierorderitem soi
                WHERE soi.supplier_order_id = so.id
                  AND soi.customer_order_item_id IS NOT NULL
            ) THEN 'CUSTOMER_ORDER'
            ELSE 'SEARCH_OFFERS'
        END
        """
    )

    op.execute(
        """
        UPDATE supplierorderitem soi
        SET
            oem_number = COALESCE(
                (
                    SELECT coi.oem
                    FROM customerorderitem coi
                    WHERE coi.id = soi.customer_order_item_id
                ),
                (
                    SELECT ap.oem_number
                    FROM autopart ap
                    WHERE ap.id = soi.autopart_id
                )
            ),
            brand_name = COALESCE(
                (
                    SELECT coi.brand
                    FROM customerorderitem coi
                    WHERE coi.id = soi.customer_order_item_id
                ),
                (
                    SELECT b.name
                    FROM autopart ap
                    JOIN brand b ON b.id = ap.brand_id
                    WHERE ap.id = soi.autopart_id
                )
            ),
            autopart_name = COALESCE(
                (
                    SELECT coi.name
                    FROM customerorderitem coi
                    WHERE coi.id = soi.customer_order_item_id
                ),
                (
                    SELECT ap.name
                    FROM autopart ap
                    WHERE ap.id = soi.autopart_id
                )
            ),
            received_quantity = CASE
                WHEN (
                    SELECT coi.ship_qty
                    FROM customerorderitem coi
                    WHERE coi.id = soi.customer_order_item_id
                ) IS NOT NULL
                AND (
                    SELECT coi.ship_qty
                    FROM customerorderitem coi
                    WHERE coi.id = soi.customer_order_item_id
                ) > 0
                    THEN (
                        SELECT coi.ship_qty
                        FROM customerorderitem coi
                        WHERE coi.id = soi.customer_order_item_id
                    )
                ELSE NULL
            END
        """
    )

    op.execute(
        """
        UPDATE orderitem oi
        SET
            oem_number = ap.oem_number,
            brand_name = b.name,
            autopart_name = ap.name
        FROM autopart ap
        LEFT JOIN brand b ON b.id = ap.brand_id
        WHERE ap.id = oi.autopart_id
        """
    )

    op.alter_column('supplierorder', 'source_type', server_default=None)
    op.alter_column('order', 'source_type', server_default=None)


def downgrade() -> None:
    op.drop_index('ix_orderitem_autopart_id', table_name='orderitem')
    op.drop_index('ix_orderitem_oem_number', table_name='orderitem')
    op.drop_column('orderitem', 'received_at')
    op.drop_column('orderitem', 'received_quantity')
    op.drop_column('orderitem', 'max_delivery_day')
    op.drop_column('orderitem', 'min_delivery_day')
    op.drop_column('orderitem', 'autopart_name')
    op.drop_column('orderitem', 'brand_name')
    op.drop_column('orderitem', 'oem_number')

    op.drop_index('ix_order_source_type', table_name='order')
    op.drop_index('ix_order_created_at', table_name='order')
    op.drop_constraint('fk_order_created_by_user', 'order', type_='foreignkey')
    op.drop_column('order', 'created_by_user_id')
    op.drop_column('order', 'source_type')

    op.drop_index(
        'ix_supplierorderitem_autopart_id', table_name='supplierorderitem'
    )
    op.drop_index(
        'ix_supplierorderitem_oem_number', table_name='supplierorderitem'
    )
    op.drop_column('supplierorderitem', 'received_at')
    op.drop_column('supplierorderitem', 'received_quantity')
    op.drop_column('supplierorderitem', 'max_delivery_day')
    op.drop_column('supplierorderitem', 'min_delivery_day')
    op.drop_column('supplierorderitem', 'autopart_name')
    op.drop_column('supplierorderitem', 'brand_name')
    op.drop_column('supplierorderitem', 'oem_number')

    op.drop_index('ix_supplierorder_source_type', table_name='supplierorder')
    op.drop_index('ix_supplierorder_created_at', table_name='supplierorder')
    op.drop_constraint(
        'fk_supplierorder_created_by_user',
        'supplierorder',
        type_='foreignkey',
    )
    op.drop_column('supplierorder', 'created_by_user_id')
    op.drop_column('supplierorder', 'source_type')
