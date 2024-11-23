"""Remove id column from price_list_autopart_association

Revision ID: 42995af4f8b3
Revises: e76b25da6ed7
Create Date: 2024-11-15 13:01:30.751475

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '42995af4f8b3'
down_revision: Union[str, None] = 'e76b25da6ed7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:

    with op.batch_alter_table("pricelistautopartassociation") as batch_op:
        # Попытка удалить существующий первичный ключ
        batch_op.drop_constraint('pricelistautopartassociation_pkey', type_='primary')

        # Удаление колонки 'id'
        batch_op.drop_column('id')

        # Создание нового первичного ключа на 'pricelist_id' и 'autopart_id'
        batch_op.create_primary_key(
            'pk_pricelistautopartassociation',
            ['pricelist_id', 'autopart_id']
        )


def downgrade() -> None:
    # Восстановление колонки 'id' как первичного ключа
    with op.batch_alter_table("pricelistautopartassociation") as batch_op:
        # Удаление нового первичного ключа
        batch_op.drop_constraint('pk_pricelistautopartassociation', type_='primary')

        # Добавление колонки 'id'
        batch_op.add_column(sa.Column('id', sa.Integer(), nullable=False))

        # Установка 'id' как первичного ключа
        batch_op.create_primary_key(
            'pricelistautopartassociation_pkey',
            ['id']
        )
