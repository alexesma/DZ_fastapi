"""Hotfix: Handle duplicate unique_autopart_category constraint

Revision ID: 852882513e8e
Revises: 45f3c75242f6
Create Date: 2025-09-01 17:07:09.236927

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text, inspect



# revision identifiers, used by Alembic.
revision: str = '852882513e8e'
down_revision: Union[str, None] = '45f3c75242f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) есть ли уже констрейнт с таким именем?
    exists_constraint = conn.exec_driver_sql(
        """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'autopart_category_association'
          AND c.conname = 'unique_autopart_category'
        LIMIT 1
        """
    ).scalar()

    if exists_constraint:
        # уже есть нужный констрейнт — выходим
        return

    # 2) может быть, уже есть индекс с таким же именем
    exists_index = conn.exec_driver_sql(
        """
        SELECT 1
        FROM pg_class
        WHERE relname = 'unique_autopart_category'
          AND relkind IN ('i','I') -- index
        LIMIT 1
        """
    ).scalar()

    if exists_index:
        # освобождаем имя под констрейнт
        conn.exec_driver_sql("DROP INDEX IF EXISTS unique_autopart_category")

    # 3) теперь создаём уникальный констрейнт
    op.create_unique_constraint(
        "unique_autopart_category",
        "autopart_category_association",
        ["autopart_id", "category_id"],
    )


def downgrade() -> None:
    # удаляем только сам констрейнт (индекс, если его создавал констрейнт, удалится автоматически)
    op.drop_constraint(
        "unique_autopart_category",
        "autopart_category_association",
        type_="unique",
    )