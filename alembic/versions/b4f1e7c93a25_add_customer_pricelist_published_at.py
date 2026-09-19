"""Отметка «прайс клиента стал действующим»

Подбор под заказ подставляет наш аналог вместо заказанного номера
только по алиасам из прайса с проставленным sent_at. Клиент, которому
прайс не рассылается (заказывает через сайт), такой отметки не получал
никогда — и заказ уходил в отказ при товаре на складе.

Отправка и «прайс действует» — разные события. Новое поле отмечает
второе: оно проставляется и при успешной отправке, и когда рассылка не
выполнялась из-за отсутствия получателей. Существующие строки не
заполняем: подбор умеет читать и старый sent_at.

Revision ID: b4f1e7c93a25
Revises: e5a8c2d4f6b1
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b4f1e7c93a25"
down_revision: Union[str, None] = "e5a8c2d4f6b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "customerpricelist",
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_customerpricelist_published_at",
        "customerpricelist",
        ["published_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_customerpricelist_published_at", table_name="customerpricelist"
    )
    op.drop_column("customerpricelist", "published_at")
