"""add tnved to okpd2 correspondence table

Ни ТН ВЭД, ни ОКПД 2 поставщики не передают: в семи файлах эти колонки
пусты во всех 405 690 строках. Заполнять их придётся самим, а ОКПД 2
выводится из ТН ВЭД по официальной таблице соответствия — формулой одно
из другого не считается.

Таблица здесь только заводится, данными не наполняется: соответствие
устанавливается официальным документом, и придумывать его нельзя.

Revision ID: b7d2e93a5c18
Revises: a3c9f61b8e24
Create Date: 2026-08-29 17:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7d2e93a5c18"
down_revision: Union[str, None] = "a3c9f61b8e24"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tnvedokpd2match",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("tnved_prefix", sa.String(20), nullable=False),
        sa.Column("okpd2_code", sa.String(20), nullable=False),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("source", sa.String(255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tnved_prefix", "okpd2_code", name="unique_tnved_okpd2"
        ),
    )
    op.create_index(
        "ix_tnvedokpd2match_tnved_prefix",
        "tnvedokpd2match",
        ["tnved_prefix"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_tnvedokpd2match_tnved_prefix", table_name="tnvedokpd2match"
    )
    op.drop_table("tnvedokpd2match")
