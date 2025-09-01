"""Add Status

Revision ID: 16e4501fa957
Revises: 396a325da0ab
Create Date: 2025-07-25 17:05:00.364217

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '16e4501fa957'
down_revision: Union[str, None] = '396a325da0ab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Определение Enum-типов
type_restock_decision_status = sa.Enum(
    'NEW', 'IN_PROGRESS', 'FULFILLED', 'CANCELLED',
    name='type_restock_decision_status'
)

type_supplier_decision_status = sa.Enum(
    'NEW', 'SEND', 'CONFIRMED', 'REJECTED', 'FULFILLED', 'ERROR',
    name='type_supplier_decision_status'
)

type_send_method = sa.Enum(
    'API', 'MAIL',
    name='type_send_method'
)

type_order_item_status = sa.Enum(
    'NEW', 'CONFIRMED', 'ARRIVED', 'CANCELLED', 'ERROR',
    name='type_order_item_status'
)


def upgrade() -> None:
    # Создание Enum-типов явно
    type_restock_decision_status.create(op.get_bind(), checkfirst=True)
    type_supplier_decision_status.create(op.get_bind(), checkfirst=True)
    type_send_method.create(op.get_bind(), checkfirst=True)
    type_order_item_status.create(op.get_bind(), checkfirst=True)

    # Добавление новых колонок с использованием созданных Enum
    op.add_column('autopartrestockdecision', sa.Column(
        'status', type_restock_decision_status,
        nullable=True,
        server_default='NEW'
    ))

    op.add_column('autopartrestockdecisionsupplier', sa.Column(
        'status', type_supplier_decision_status,
        nullable=True,
        server_default='NEW'
    ))

    op.add_column('autopartrestockdecisionsupplier', sa.Column(
        'send_method', type_send_method,
        nullable=True,
        server_default='MAIL'
    ))

    op.add_column('autopartrestockdecisionsupplier', sa.Column(
        'send_date', sa.DateTime(),
        nullable=True
    ))

    op.add_column('orderitem', sa.Column(
        'status', type_order_item_status,
        nullable=True,
        server_default='NEW'
    ))


def downgrade() -> None:
    # Удаление колонок
    op.drop_column('orderitem', 'status')
    op.drop_column('autopartrestockdecisionsupplier', 'send_date')
    op.drop_column('autopartrestockdecisionsupplier', 'send_method')
    op.drop_column('autopartrestockdecisionsupplier', 'status')
    op.drop_column('autopartrestockdecision', 'status')

    # Удаление Enum-типов явно
    type_order_item_status.drop(op.get_bind(), checkfirst=True)
    type_send_method.drop(op.get_bind(), checkfirst=True)
    type_supplier_decision_status.drop(op.get_bind(), checkfirst=True)
    type_restock_decision_status.drop(op.get_bind(), checkfirst=True)
