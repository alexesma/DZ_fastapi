"""Change CheckConstraint for brand

Revision ID: 6ecf1f115624
Revises: f031465ac9e8
Create Date: 2024-07-17 15:41:26.070437

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql.expression import text


# revision identifiers, used by Alembic.
revision: str = '6ecf1f115624'
down_revision: Union[str, None] = 'f031465ac9e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # Удаляем старое ограничение
    op.drop_constraint('check_name_brand', 'brand', type_='check')
    # Создаем новое ограничение с обновленным регулярным выражением
    op.create_check_constraint('check_name_brand', 'brand', text(r"name ~ '^[a-zA-Z0-9]+(?:[ -]?[a-zA-Z0-9]+)*$'"))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    # Удаляем новое ограничение
    op.drop_constraint('check_name_brand', 'brand', type_='check')
    # Восстанавливаем старое ограничение
    op.create_check_constraint('check_name_brand', 'brand', text(r"name ~ '^[a-zA-Z0-9-]+$'"))
    # ### end Alembic commands ###