"""Правила «не требует сертификации» по наименованию.

Два узких места закрыты тестами намеренно:

* уточнение должно перебивать общее правило — натяжной ролик и
  натяжитель ГРМ проходят оценку соответствия, а такие же по названию
  элементы привода навесного оборудования нет;
* латинские буквы-двойники — в присланных списках встречаются «Cальник»
  с латинской C и «Щyп ypoвня мacлa», которые без приведения не совпали
  бы ни с одним наименованием.
"""
import pytest
import sqlalchemy as sa
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.services.certification_rules import (
    apply_exemption_rules,
    match_rule,
    normalize_name,
    sync_exemption_rules,
)

RULES = [
    ("сальник", False),
    ("натяжитель ремня", False),
    ("натяжитель ремня грм", True),
    ("щуп уровня масла", False),
]


# ── нормализация ────────────────────────────────────────────────────────


def test_latin_lookalikes_become_cyrillic():
    # «Cальник» здесь начинается с латинской C.
    assert normalize_name("Cальник") == "сальник"
    # «Щyп ypoвня мacлa» — латинские y, p, o, a, c.
    assert normalize_name("Щyп ypoвня мacлa") == "щуп уровня масла"
    assert normalize_name("Кольцо уплотнительноe") == "кольцо уплотнительное"


def test_separators_and_case_are_flattened():
    assert normalize_name("Сальник  |  КПП") == "сальник кпп"
    assert normalize_name("САЛЬНИК, двигателя") == "сальник двигателя"
    assert normalize_name(None) == ""


# ── выбор правила ───────────────────────────────────────────────────────


def test_longer_pattern_wins_over_general():
    assert match_rule("Натяжитель ремня ГРМ TOYOTA", RULES) == (
        "натяжитель ремня грм",
        True,
    )
    assert match_rule("Натяжитель ремня приводного", RULES) == (
        "натяжитель ремня",
        False,
    )


def test_homoglyph_name_still_matches():
    assert match_rule("Cальник КПП", RULES) == ("сальник", False)


def test_no_match_returns_none():
    assert match_rule("Ролик натяжной приводного ремня", RULES) is None
    assert match_rule("", RULES) is None


# ── применение к каталогу ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_rules_seeded_once(test_session):
    first = await sync_exemption_rules(test_session)
    assert first["created"] > 0
    second = await sync_exemption_rules(test_session)
    assert second["created"] == 0


@pytest.mark.asyncio
async def test_apply_sets_flag_and_respects_existing(
    test_session, created_brand
):
    await sync_exemption_rules(test_session)
    exempt = AutoPart(
        name="Сальник КПП", brand_id=created_brand.id, oem_number="RULE1"
    )
    grm = AutoPart(
        name="Натяжитель ремня ГРМ", brand_id=created_brand.id,
        oem_number="RULE2",
    )
    from_supplier = AutoPart(
        name="Сальник двигателя",
        brand_id=created_brand.id,
        oem_number="RULE3",
        certification_required=True,
        regulatory_source="supplier_doc",
    )
    test_session.add_all([exempt, grm, from_supplier])
    await test_session.commit()

    stats = await apply_exemption_rules(test_session, dry_run=False)
    assert stats["updated"] >= 2

    await test_session.refresh(exempt)
    await test_session.refresh(grm)
    await test_session.refresh(from_supplier)
    assert exempt.certification_required is False
    assert exempt.regulatory_source == "rule"
    # Уточнение ГРМ перебило общее правило про натяжитель ремня.
    assert grm.certification_required is True
    # Данные поставщика правилом не перетёрты.
    assert from_supplier.regulatory_source == "supplier_doc"


@pytest.mark.asyncio
async def test_dry_run_does_not_write(test_session, created_brand):
    await sync_exemption_rules(test_session)
    part = AutoPart(
        name="Хомут глушителя", brand_id=created_brand.id, oem_number="RULE4"
    )
    test_session.add(part)
    await test_session.commit()

    stats = await apply_exemption_rules(test_session, dry_run=True)
    assert stats["updated"] >= 1
    await test_session.refresh(part)
    assert part.certification_required is None


@pytest.mark.asyncio
async def test_sql_normalization_matches_python(test_session):
    """Миграция нормализует наименования на SQL, сервис — на Python.

    Расхождение означало бы, что прод и приложение проставляют признак
    по-разному, поэтому сверяем реализации на трудных строках.
    """
    samples = [
        "Cальник КПП",
        "Щyп ypoвня мacлa",
        "Кольцо уплотнительноe",
        "Натяжитель  ремня   ГРМ",
        "САЛЬНИК, двигателя | 2.0",
        "Болт-М10х1.25",
        "",
    ]
    sql = sa.text(
        """
        SELECT regexp_replace(
                 regexp_replace(
                   translate(lower(:value), 'aceopxybhkmt', 'асеорхуьнкмт'),
                   '[^0-9a-zа-яё]+', ' ', 'g'),
                 '\\s+', ' ', 'g')
        """
    )
    for value in samples:
        got = (await test_session.execute(sql, {"value": value})).scalar()
        assert (got or "").strip() == normalize_name(value), value


@pytest.mark.asyncio
async def test_unmatched_parts_stay_undetermined(
    test_session, created_brand
):
    await sync_exemption_rules(test_session)
    part = AutoPart(
        name="Ролик натяжной приводного ремня",
        brand_id=created_brand.id,
        oem_number="RULE5",
    )
    test_session.add(part)
    await test_session.commit()

    await apply_exemption_rules(test_session, dry_run=False)
    stored = (
        await test_session.execute(
            select(AutoPart).where(AutoPart.oem_number == "RULE5")
        )
    ).scalar_one()
    # «Ролик» в список не входит — признак остаётся неопределённым,
    # а не проставляется наугад.
    assert stored.certification_required is None
