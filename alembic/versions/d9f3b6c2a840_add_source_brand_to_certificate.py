"""add source_brand to certificate and match brands by normalized key

Бренд из прайса поставщика сохраняем на сертификате как есть: заводить
ради документа бренд, которым мы не торгуем, нельзя — он попадёт в
фильтры прайсов, кроссы и подбор. Заодно доставляем brand_id тем
документам, где написание отличалось только регистром и разделителями
(«Hyundai/Kia» против «HYUNDAI-KIA»).

Revision ID: d9f3b6c2a840
Revises: c8e4a17b93d5
Create Date: 2026-08-05 16:00:00.000000

"""
import csv
import re
from pathlib import Path
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d9f3b6c2a840"
down_revision: Union[str, None] = "c8e4a17b93d5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "regulatory"


def _brand_key(value: str) -> str:
    """Ключ сравнения брендов: регистр и разделители у поставщиков разные."""
    return re.sub(r"[^0-9a-zа-яё]", "", (value or "").lower())


def _seed_brands() -> list[tuple[str, str]]:
    """Номер сертификата и бренд так, как он записан в файле поставщика."""
    path = DATA_DIR / "certificates.csv"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as handle:
        return [
            (row["number"].strip(), (row.get("brand") or "").strip())
            for row in csv.DictReader(handle)
            if row.get("number") and (row.get("brand") or "").strip()
        ]


def upgrade() -> None:
    op.add_column(
        "certificate", sa.Column("source_brand", sa.String(255), nullable=True)
    )
    op.create_index(
        "ix_certificate_source_brand", "certificate", ["source_brand"]
    )

    # Документам без бренда, но со связями, проставляем бренд, если все
    # привязанные позиции одного бренда: связи в каталоге точнее, чем
    # колонка в чужом файле.
    op.execute(
        """
        UPDATE certificate c
           SET brand_id = t.brand_id
          FROM (
            SELECT aca.certificate_id,
                   min(a.brand_id) AS brand_id,
                   count(DISTINCT a.brand_id) AS brands
              FROM autopart_certificate_association aca
              JOIN autopart a ON a.id = aca.autopart_id
             GROUP BY aca.certificate_id
          ) t
         WHERE t.certificate_id = c.id
           AND t.brands = 1
           AND c.brand_id IS NULL
        """
    )

    _backfill_from_seed()

    # Бренд для отображения: у документов со связями берём каноничное имя
    # из каталога, чтобы поиск работал единообразно.
    op.execute(
        """
        UPDATE certificate c
           SET source_brand = b.name
          FROM brand b
         WHERE b.id = c.brand_id
           AND c.source_brand IS NULL
        """
    )


def _backfill_from_seed() -> None:
    """Достаёт бренд из того же файла, из которого пришли сертификаты.

    Сид сопоставлял бренд точным ``lower(name)``, поэтому «Hyundai/Kia»
    мимо каталожного «HYUNDAI-KIA» проходил, и документ оставался без
    бренда. Здесь сравниваем по нормализованному ключу, а исходное
    написание сохраняем — по нему видно, чей это документ, даже когда
    такого бренда у нас нет.
    """
    seed = _seed_brands()
    if not seed:
        return

    bind = op.get_bind()
    brands = {
        _brand_key(name): brand_id
        for brand_id, name in bind.execute(
            sa.text("SELECT id, name FROM brand")
        )
    }
    payload = [
        {
            "number": number,
            "source_brand": brand,
            "brand_id": brands.get(_brand_key(brand)),
        }
        for number, brand in seed
    ]

    op.execute("DROP TABLE IF EXISTS _certificate_brand_stage")
    op.execute(
        """
        CREATE TABLE _certificate_brand_stage (
            number text, source_brand text, brand_id integer
        )
        """
    )
    bind.execute(
        sa.text(
            "INSERT INTO _certificate_brand_stage "
            "(number, source_brand, brand_id) "
            "VALUES (:number, :source_brand, :brand_id)"
        ),
        payload,
    )
    op.execute("CREATE INDEX ON _certificate_brand_stage (number)")

    op.execute(
        """
        UPDATE certificate c
           SET source_brand = t.source_brand
          FROM _certificate_brand_stage t
         WHERE t.number = c.number
           AND c.source_brand IS NULL
        """
    )
    op.execute(
        """
        UPDATE certificate c
           SET brand_id = t.brand_id
          FROM _certificate_brand_stage t
         WHERE t.number = c.number
           AND t.brand_id IS NOT NULL
           AND c.brand_id IS NULL
        """
    )
    op.execute("DROP TABLE IF EXISTS _certificate_brand_stage")


def downgrade() -> None:
    op.drop_index("ix_certificate_source_brand", table_name="certificate")
    op.drop_column("certificate", "source_brand")
