"""Add external status mapping tables

Revision ID: b3d4e5f6a7b8
Revises: a8b7c6d5e4f3
Create Date: 2026-04-03 21:05:00.000000

"""

from datetime import datetime, timezone
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = 'b3d4e5f6a7b8'
down_revision: Union[str, None] = 'a8b7c6d5e4f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'external_status_mapping',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('source_key', sa.String(length=64), nullable=False),
        sa.Column('provider_id', sa.Integer(), nullable=True),
        sa.Column(
            'match_mode',
            sa.Enum(
                'EXACT',
                'CONTAINS',
                name='externalstatusmatchmode',
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column('raw_status', sa.String(length=255), nullable=False),
        sa.Column(
            'normalized_status', sa.String(length=255), nullable=False
        ),
        sa.Column(
            'internal_order_status', sa.String(length=64), nullable=True
        ),
        sa.Column(
            'internal_item_status', sa.String(length=64), nullable=True
        ),
        sa.Column('priority', sa.Integer(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_by_user_id', sa.Integer(), nullable=True),
        sa.Column('updated_by_user_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['provider_id'], ['provider.id']),
        sa.ForeignKeyConstraint(['created_by_user_id'], ['app_user.id']),
        sa.ForeignKeyConstraint(['updated_by_user_id'], ['app_user.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_external_status_mapping_source_key',
        'external_status_mapping',
        ['source_key'],
    )
    op.create_index(
        'ix_external_status_mapping_provider_id',
        'external_status_mapping',
        ['provider_id'],
    )
    op.create_index(
        'ix_external_status_mapping_normalized_status',
        'external_status_mapping',
        ['normalized_status'],
    )
    op.create_index(
        'ix_external_status_mapping_is_active',
        'external_status_mapping',
        ['is_active'],
    )

    op.create_table(
        'external_status_unmapped',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('source_key', sa.String(length=64), nullable=False),
        sa.Column('provider_id', sa.Integer(), nullable=True),
        sa.Column('raw_status', sa.String(length=255), nullable=False),
        sa.Column(
            'normalized_status', sa.String(length=255), nullable=False
        ),
        sa.Column('seen_count', sa.Integer(), nullable=False),
        sa.Column('first_seen_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('last_seen_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('sample_order_id', sa.Integer(), nullable=True),
        sa.Column('sample_item_id', sa.Integer(), nullable=True),
        sa.Column('sample_payload', sa.JSON(), nullable=True),
        sa.Column('is_resolved', sa.Boolean(), nullable=False),
        sa.Column('mapping_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['provider_id'], ['provider.id']),
        sa.ForeignKeyConstraint(['sample_order_id'], ['order.id']),
        sa.ForeignKeyConstraint(['sample_item_id'], ['orderitem.id']),
        sa.ForeignKeyConstraint(
            ['mapping_id'], ['external_status_mapping.id']
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_external_status_unmapped_source_key',
        'external_status_unmapped',
        ['source_key'],
    )
    op.create_index(
        'ix_external_status_unmapped_provider_id',
        'external_status_unmapped',
        ['provider_id'],
    )
    op.create_index(
        'ix_external_status_unmapped_normalized_status',
        'external_status_unmapped',
        ['normalized_status'],
    )
    op.create_index(
        'ix_external_status_unmapped_sample_order_id',
        'external_status_unmapped',
        ['sample_order_id'],
    )
    op.create_index(
        'ix_external_status_unmapped_sample_item_id',
        'external_status_unmapped',
        ['sample_item_id'],
    )
    op.create_index(
        'ix_external_status_unmapped_mapping_id',
        'external_status_unmapped',
        ['mapping_id'],
    )
    op.create_index(
        'ix_external_status_unmapped_is_resolved',
        'external_status_unmapped',
        ['is_resolved'],
    )

    op.add_column(
        'orderitem',
        sa.Column(
            'external_status_source', sa.String(length=64), nullable=True
        ),
    )
    op.add_column(
        'orderitem',
        sa.Column('external_status_raw', sa.Text(), nullable=True),
    )
    op.add_column(
        'orderitem',
        sa.Column(
            'external_status_normalized',
            sa.String(length=255),
            nullable=True,
        ),
    )
    op.add_column(
        'orderitem',
        sa.Column(
            'external_status_synced_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        'orderitem',
        sa.Column('external_status_mapping_id', sa.Integer(), nullable=True),
    )
    op.create_index(
        op.f('ix_orderitem_external_status_source'),
        'orderitem',
        ['external_status_source'],
    )
    op.create_index(
        op.f('ix_orderitem_external_status_normalized'),
        'orderitem',
        ['external_status_normalized'],
    )
    op.create_index(
        op.f('ix_orderitem_external_status_mapping_id'),
        'orderitem',
        ['external_status_mapping_id'],
    )
    op.create_foreign_key(
        'fk_orderitem_external_status_mapping_id',
        'orderitem',
        'external_status_mapping',
        ['external_status_mapping_id'],
        ['id'],
    )

    mapping_table = sa.table(
        'external_status_mapping',
        sa.column('source_key', sa.String),
        sa.column('provider_id', sa.Integer),
        sa.column('match_mode', sa.String),
        sa.column('raw_status', sa.String),
        sa.column('normalized_status', sa.String),
        sa.column('internal_order_status', sa.String),
        sa.column('internal_item_status', sa.String),
        sa.column('priority', sa.Integer),
        sa.column('is_active', sa.Boolean),
        sa.column('notes', sa.Text),
        sa.column('created_at', sa.DateTime(timezone=True)),
        sa.column('updated_at', sa.DateTime(timezone=True)),
    )
    now = datetime.now(timezone.utc)
    default_rules = [
        ('refusal', 'REFUSAL', 'CANCELLED'),
        ('refused', 'REFUSAL', 'CANCELLED'),
        ('rejected', 'REFUSAL', 'CANCELLED'),
        ('declined', 'REFUSAL', 'CANCELLED'),
        ('отказ', 'REFUSAL', 'CANCELLED'),
        ('return', 'RETURNED', 'CANCELLED'),
        ('returned', 'RETURNED', 'CANCELLED'),
        ('возврат', 'RETURNED', 'CANCELLED'),
        ('removed', 'REMOVED', 'CANCELLED'),
        ('deleted', 'REMOVED', 'CANCELLED'),
        ('cancelled by supplier', 'REMOVED', 'CANCELLED'),
        ('снят', 'REMOVED', 'CANCELLED'),
        ('удален', 'REMOVED', 'CANCELLED'),
        ('error', 'ERROR', 'ERROR'),
        ('failed', 'ERROR', 'ERROR'),
        ('failure', 'ERROR', 'ERROR'),
        ('ошибка', 'ERROR', 'ERROR'),
        ('shipped', 'SHIPPED', 'DELIVERED'),
        ('issued', 'SHIPPED', 'DELIVERED'),
        ('delivered', 'SHIPPED', 'DELIVERED'),
        ('completed', 'SHIPPED', 'DELIVERED'),
        ('done', 'SHIPPED', 'DELIVERED'),
        ('выдан', 'SHIPPED', 'DELIVERED'),
        ('получен', 'SHIPPED', 'DELIVERED'),
        ('доставлен', 'SHIPPED', 'DELIVERED'),
        ('arrived', 'ARRIVED', 'IN_PROGRESS'),
        ('ready', 'ARRIVED', 'IN_PROGRESS'),
        ('готов', 'ARRIVED', 'IN_PROGRESS'),
        ('прибыл', 'ARRIVED', 'IN_PROGRESS'),
        ('accepted', 'ACCEPTED', 'CONFIRMED'),
        ('принят', 'ACCEPTED', 'CONFIRMED'),
        ('transit', 'TRANSIT', 'IN_PROGRESS'),
        ('in transit', 'TRANSIT', 'IN_PROGRESS'),
        ('shipping', 'TRANSIT', 'IN_PROGRESS'),
        ('on way', 'TRANSIT', 'IN_PROGRESS'),
        ('в пути', 'TRANSIT', 'IN_PROGRESS'),
        ('confirm', 'CONFIRMED', 'CONFIRMED'),
        ('approved', 'CONFIRMED', 'CONFIRMED'),
        ('подтверж', 'CONFIRMED', 'CONFIRMED'),
        ('processing', 'PROCESSING', 'IN_PROGRESS'),
        ('assembly', 'PROCESSING', 'IN_PROGRESS'),
        ('reserved', 'PROCESSING', 'IN_PROGRESS'),
        ('обрабаты', 'PROCESSING', 'IN_PROGRESS'),
        ('собира', 'PROCESSING', 'IN_PROGRESS'),
        ('резерв', 'PROCESSING', 'IN_PROGRESS'),
        ('ordered', 'ORDERED', 'SENT'),
        ('created', 'ORDERED', 'SENT'),
        ('new order', 'ORDERED', 'SENT'),
        ('новый заказ', 'ORDERED', 'SENT'),
        ('заказан', 'ORDERED', 'SENT'),
        ('создан', 'ORDERED', 'SENT'),
    ]
    op.bulk_insert(
        mapping_table,
        [
            {
                'source_key': 'DRAGONZAP_SITE',
                'provider_id': None,
                'match_mode': 'CONTAINS',
                'raw_status': raw_status,
                'normalized_status': raw_status,
                'internal_order_status': order_status,
                'internal_item_status': item_status,
                'priority': 100,
                'is_active': True,
                'notes': 'Seeded from legacy Dragonzap status matching',
                'created_at': now,
                'updated_at': now,
            }
            for raw_status, order_status, item_status in default_rules
        ],
    )


def downgrade() -> None:
    op.drop_constraint(
        'fk_orderitem_external_status_mapping_id',
        'orderitem',
        type_='foreignkey',
    )
    op.drop_index(
        op.f('ix_orderitem_external_status_mapping_id'),
        table_name='orderitem',
    )
    op.drop_index(
        op.f('ix_orderitem_external_status_normalized'),
        table_name='orderitem',
    )
    op.drop_index(
        op.f('ix_orderitem_external_status_source'),
        table_name='orderitem',
    )
    op.drop_column('orderitem', 'external_status_mapping_id')
    op.drop_column('orderitem', 'external_status_synced_at')
    op.drop_column('orderitem', 'external_status_normalized')
    op.drop_column('orderitem', 'external_status_raw')
    op.drop_column('orderitem', 'external_status_source')

    op.drop_index(
        'ix_external_status_unmapped_is_resolved',
        table_name='external_status_unmapped',
    )
    op.drop_index(
        'ix_external_status_unmapped_mapping_id',
        table_name='external_status_unmapped',
    )
    op.drop_index(
        'ix_external_status_unmapped_sample_item_id',
        table_name='external_status_unmapped',
    )
    op.drop_index(
        'ix_external_status_unmapped_sample_order_id',
        table_name='external_status_unmapped',
    )
    op.drop_index(
        'ix_external_status_unmapped_normalized_status',
        table_name='external_status_unmapped',
    )
    op.drop_index(
        'ix_external_status_unmapped_provider_id',
        table_name='external_status_unmapped',
    )
    op.drop_index(
        'ix_external_status_unmapped_source_key',
        table_name='external_status_unmapped',
    )
    op.drop_table('external_status_unmapped')

    op.drop_index(
        'ix_external_status_mapping_is_active',
        table_name='external_status_mapping',
    )
    op.drop_index(
        'ix_external_status_mapping_normalized_status',
        table_name='external_status_mapping',
    )
    op.drop_index(
        'ix_external_status_mapping_provider_id',
        table_name='external_status_mapping',
    )
    op.drop_index(
        'ix_external_status_mapping_source_key',
        table_name='external_status_mapping',
    )
    op.drop_table('external_status_mapping')
