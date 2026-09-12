"""Кэш показателей прайсов для дашборда

Раздел «Динамика прайсов поставщиков» пересчитывал count, sum и avg по
9,4 млн строк таблицы связей при каждом открытии дашборда и не укладывался
в тридцатисекундный таймаут браузера — запрос обрывался, и вместо графика
появлялось «Часть сводки временно недоступна». Показатели загруженного
прайса и сравнение двух соседних прайсов после загрузки не меняются,
поэтому считаем их один раз и храним: четыре числа на прайс и три на пару.

Заполнять существующие прайсы миграция не пытается — кэш наполняется по
мере обращений, а до этого запрос работает как раньше.

Revision ID: c5e8b2a71d43
Revises: a1c4e7b9d2f6
"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c5e8b2a71d43"
down_revision: Union[str, None] = "a1c4e7b9d2f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pricelistmetriccache",
        sa.Column("pricelist_id", sa.Integer(), nullable=False),
        sa.Column(
            "total_sku_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("sku_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "stock_total_qty",
            sa.BigInteger(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("avg_price", sa.Float(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["pricelist_id"], ["pricelist.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("pricelist_id"),
    )
    op.create_table(
        "pricelistpairstatcache",
        sa.Column("prev_pricelist_id", sa.Integer(), nullable=False),
        sa.Column("curr_pricelist_id", sa.Integer(), nullable=False),
        sa.Column(
            "overlap_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("median_pct", sa.Float(), nullable=True),
        sa.Column("changed_share_pct", sa.Float(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["prev_pricelist_id"], ["pricelist.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["curr_pricelist_id"], ["pricelist.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("prev_pricelist_id", "curr_pricelist_id"),
    )


def downgrade() -> None:
    op.drop_table("pricelistpairstatcache")
    op.drop_table("pricelistmetriccache")
