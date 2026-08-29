"""add regulatory column pointers to provider pricelist config

Реквизиты приходят каждый день в тех же прайсах, что и цены с остатками,
но приём читал ровно семь колонок и всё остальное отбрасывал. Из-за
этого данные попадали в систему только разовой ручной загрузкой и
устаревали вместе с ассортиментом.

Все пять колонок необязательные: у поставщика, который их не передаёт,
приём прайса не меняется.

Revision ID: a3c9f61b8e24
Revises: f2b8d4e0a71c
Create Date: 2026-08-29 15:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a3c9f61b8e24"
down_revision: Union[str, None] = "f2b8d4e0a71c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

COLUMNS = (
    "tnved_col",
    "okpd2_col",
    "honest_sign_col",
    "eac_cert_col",
    "eac_cert_url_col",
)


def upgrade() -> None:
    for column in COLUMNS:
        op.add_column(
            "providerpricelistconfig",
            sa.Column(column, sa.Integer(), nullable=True),
        )


def downgrade() -> None:
    for column in COLUMNS:
        op.drop_column("providerpricelistconfig", column)
