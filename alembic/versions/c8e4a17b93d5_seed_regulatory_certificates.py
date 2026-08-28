"""seed certificates, part links and certification exemption rules

Данные лежат в data/regulatory и едут вместе с репозиторием, поэтому
после git pull и перезапуска контейнера прод получает их автоматически:
entrypoint выполняет alembic upgrade heads на старте.

Привязка идёт по натуральному ключу «бренд + нормализованный артикул»,
а не по id номенклатуры: id в dev и на проде разные. Позиции, которых
на целевой базе нет, просто не сматчатся — это нормально.

Revision ID: c8e4a17b93d5
Revises: b6c2f8d41e37
Create Date: 2026-08-05 14:00:00.000000

"""
import csv
import gzip
from pathlib import Path
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c8e4a17b93d5"
down_revision: Union[str, None] = "b6c2f8d41e37"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "regulatory"
BATCH = 5000


def _load_certificates() -> list[dict]:
    path = DATA_DIR / "certificates.csv"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as handle:
        return [
            {
                "number": row["number"].strip(),
                "url": (row.get("url") or "").strip() or None,
                "brand": (row.get("brand") or "").strip().lower() or None,
            }
            for row in csv.DictReader(handle)
            if row.get("number")
        ]


def _load_rules() -> list[dict]:
    path = DATA_DIR / "exemption_rules.csv"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as handle:
        return [
            {
                "pattern": row["pattern"],
                "normalized_pattern": row["normalized_pattern"],
                "required": row["required"] == "1",
            }
            for row in csv.DictReader(handle)
            if row.get("normalized_pattern")
        ]


def _iter_links():
    path = DATA_DIR / "part_certificates.csv.gz"
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            brand = (row.get("brand") or "").strip().lower()
            article = (row.get("article") or "").strip()
            if not brand or not article:
                continue
            yield brand, article, (row.get("certificate") or "").strip()


def upgrade() -> None:
    bind = op.get_bind()

    # ── справочник правил ────────────────────────────────────────────────
    rules = _load_rules()
    if rules:
        bind.execute(
            sa.text(
                """
                INSERT INTO certificationexemptionrule
                    (pattern, normalized_pattern, certification_required,
                     is_active, created_at)
                VALUES (:pattern, :normalized_pattern, :required, true, now())
                ON CONFLICT (normalized_pattern, certification_required)
                DO NOTHING
                """
            ),
            rules,
        )

    # ── сертификаты ─────────────────────────────────────────────────────
    certificates = _load_certificates()
    if certificates:
        bind.execute(
            sa.text(
                """
                INSERT INTO certificate
                    (number, url, brand_id, covers_whole_brand, source,
                     created_at, updated_at)
                SELECT :number, :url,
                       (SELECT id FROM brand
                         WHERE lower(name) = :brand LIMIT 1),
                       false, 'supplier_file', now(), now()
                ON CONFLICT (number) DO NOTHING
                """
            ),
            certificates,
        )

    # ── связи и признак «не требует» ────────────────────────────────────
    # Временная таблица + join одним набором: построчный ORM на 152 тыс.
    # строк занял бы минуты и заблокировал деплой.
    bind.execute(
        sa.text(
            """
            CREATE TEMP TABLE tmp_regulatory_seed (
                brand text, article text, certificate text
            ) ON COMMIT DROP
            """
        )
    )
    batch: list[dict] = []
    insert_tmp = sa.text(
        "INSERT INTO tmp_regulatory_seed (brand, article, certificate) "
        "VALUES (:brand, :article, :certificate)"
    )
    for brand, article, certificate in _iter_links():
        batch.append(
            {"brand": brand, "article": article, "certificate": certificate}
        )
        if len(batch) >= BATCH:
            bind.execute(insert_tmp, batch)
            batch = []
    if batch:
        bind.execute(insert_tmp, batch)

    bind.execute(
        sa.text(
            "CREATE INDEX ON tmp_regulatory_seed (brand, article)"
        )
    )

    # Связь позиции с сертификатом.
    bind.execute(
        sa.text(
            """
            INSERT INTO autopart_certificate_association
                (autopart_id, certificate_id)
            SELECT DISTINCT a.id, c.id
            FROM tmp_regulatory_seed t
            JOIN brand b ON lower(b.name) = t.brand
            JOIN autopart a
              ON a.brand_id = b.id AND a.oem_number = t.article
            JOIN certificate c ON c.number = t.certificate
            WHERE t.certificate <> ''
            ON CONFLICT DO NOTHING
            """
        )
    )

    # Кэш действующего документа на карточке — из него собирается прайс.
    # Ручной ввод не трогаем.
    bind.execute(
        sa.text(
            """
            UPDATE autopart a
               SET eac_cert_number = c.number,
                   eac_cert_url = COALESCE(a.eac_cert_url, c.url),
                   certification_required = true,
                   regulatory_source = COALESCE(a.regulatory_source,
                                                'supplier_doc')
              FROM tmp_regulatory_seed t
              JOIN brand b ON lower(b.name) = t.brand
              JOIN certificate c ON c.number = t.certificate
             WHERE a.brand_id = b.id
               AND a.oem_number = t.article
               AND t.certificate <> ''
               AND a.eac_cert_number IS NULL
               AND COALESCE(a.regulatory_source, '') <> 'manual'
            """
        )
    )

    # Признак «не требует сертификации» — только там, где не определено.
    bind.execute(
        sa.text(
            """
            UPDATE autopart a
               SET certification_required = false,
                   regulatory_source = 'supplier_doc'
              FROM tmp_regulatory_seed t
              JOIN brand b ON lower(b.name) = t.brand
             WHERE a.brand_id = b.id
               AND a.oem_number = t.article
               AND t.certificate = ''
               AND a.certification_required IS NULL
               AND COALESCE(a.regulatory_source, '') <> 'manual'
            """
        )
    )

    # ── применение правил по наименованию ───────────────────────────────
    # Нормализация повторяет normalize_name из services/certification_rules:
    # нижний регистр, латинские буквы-двойники → кириллица, всё кроме букв
    # и цифр → пробел. Эквивалентность закреплена тестом
    # test_sql_normalization_matches_python.
    bind.execute(
        sa.text(
            """
            UPDATE autopart a
               SET certification_required = m.certification_required,
                   regulatory_source = 'rule'
              FROM (
                SELECT DISTINCT ON (p.id)
                       p.id, r.certification_required
                  FROM autopart p
                  JOIN certificationexemptionrule r
                    ON r.is_active
                   -- lower() строго до translate: иначе латинская
                   -- заглавная C в «Cальник» не попадёт под замену.
                   AND regexp_replace(
                         regexp_replace(
                           translate(lower(p.name),
                                 'aceopxybhkmt', 'асеорхуьнкмт'),
                           '[^0-9a-zа-яё]+', ' ', 'g'),
                         '\\s+', ' ', 'g')
                       LIKE '%' || r.normalized_pattern || '%'
                 WHERE p.certification_required IS NULL
                 -- Самый длинный шаблон выигрывает: уточнение «грм»
                 -- перебивает общее правило. При равной длине приоритет
                 -- у «требует» — неоднозначность в безопасную сторону.
                 ORDER BY p.id,
                          length(r.normalized_pattern) DESC,
                          r.certification_required DESC
              ) m
             WHERE a.id = m.id
               AND a.certification_required IS NULL
               AND COALESCE(a.regulatory_source, '') <> 'manual'
            """
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Снимаем только то, что проставил этот сид, ручной ввод не трогаем.
    bind.execute(
        sa.text(
            """
            DELETE FROM autopart_certificate_association
             WHERE certificate_id IN (
                   SELECT id FROM certificate WHERE source = 'supplier_file'
             )
            """
        )
    )
    bind.execute(
        sa.text(
            """
            UPDATE autopart
               SET eac_cert_number = NULL,
                   eac_cert_url = NULL,
                   certification_required = NULL,
                   regulatory_source = NULL
             WHERE regulatory_source IN ('supplier_doc', 'rule')
            """
        )
    )
    bind.execute(
        sa.text("DELETE FROM certificate WHERE source = 'supplier_file'")
    )
    bind.execute(sa.text("DELETE FROM certificationexemptionrule"))
