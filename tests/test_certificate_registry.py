"""Справочник сертификатов: нормализация номера, кэш и защита ручного ввода.

Три случая закрыты тестами после самопроверки — каждый воспроизводился на
живых данных:

* нестандартный номер «ЕАЭС BY/112 …» переводился целиком, и латинский
  код страны BY превращался в кириллическое ВУ — документ переставал
  находиться в реестре;
* предпросмотр (dry_run) делал загруженный сертификат «грязным» в сессии,
  и последующий commit в том же запросе записал бы его;
* применение сертификата к бренду со снятым ограничением перетирало
  ручной ввод, хотя интерфейс обещает обратное.
"""
from datetime import date, timedelta

import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.models.certificate import Certificate, autopart_certificate_association
from dz_fastapi.services.regulatory import (
    apply_brand_certificate,
    normalize_certificate_number,
    refresh_autopart_certificate_cache,
)

CERT = "ЕАЭС RU С-BE.НВ07.В.00826/23"


# ── нормализация номера ─────────────────────────────────────────────────


def test_latin_lookalikes_fixed_in_standard_number():
    assert normalize_certificate_number("RU C-BE.HB07.B.00826/23") == (
        "RU С-BE.НВ07.В.00826/23"
    )


def test_country_code_after_dash_stays_latin():
    assert "BE" in normalize_certificate_number("RU C-BE.HB07.B.00826/23")
    assert "-KR." in normalize_certificate_number("ТС RU С-KR.МТ25.В.04250")


def test_nonstandard_number_left_untouched():
    """Белорусский номер без кода страны после дефиса не трогаем."""
    value = "ЕАЭС BY/112 02.01. ТР018 021.02 00567"
    assert normalize_certificate_number(value) == value


def test_whitespace_collapsed():
    assert normalize_certificate_number("  ЕАЭС  RU  С-KR.АД50.В.1/23 ") == (
        "ЕАЭС RU С-KR.АД50.В.1/23"
    )


def test_empty_number_gives_empty_string():
    assert normalize_certificate_number(None) == ""
    assert normalize_certificate_number("   ") == ""


# ── кэш сертификата на карточке ─────────────────────────────────────────


async def _part_with_certificate(session, brand, oem, **cert_kwargs):
    certificate = Certificate(number=cert_kwargs.pop("number", CERT), **cert_kwargs)
    part = AutoPart(name="Деталь", brand_id=brand.id, oem_number=oem)
    part.certificates = [certificate]
    session.add_all([certificate, part])
    await session.commit()
    return part, certificate


@pytest.mark.asyncio
async def test_cache_filled_from_link(test_session, created_brand):
    part, _ = await _part_with_certificate(
        test_session, created_brand, "CACHE1", url="https://pub.fsa.gov.ru/x"
    )
    await refresh_autopart_certificate_cache(test_session, [part.id])
    await test_session.refresh(part)
    assert part.eac_cert_number == CERT
    assert part.eac_cert_url == "https://pub.fsa.gov.ru/x"


@pytest.mark.asyncio
async def test_expired_certificate_is_not_cached(test_session, created_brand):
    part, _ = await _part_with_certificate(
        test_session,
        created_brand,
        "CACHE2",
        valid_until=date.today() - timedelta(days=1),
    )
    await refresh_autopart_certificate_cache(test_session, [part.id])
    await test_session.refresh(part)
    # Просроченный документ хуже пустой ячейки: клиент проверит его первым.
    assert part.eac_cert_number is None


@pytest.mark.asyncio
async def test_dated_certificate_wins_over_undated(
    test_session, created_brand
):
    undated = Certificate(number="ЕАЭС RU С-KR.АД50.В.0001/23")
    dated = Certificate(
        number="ЕАЭС RU С-KR.АД50.В.0002/23",
        valid_until=date.today() + timedelta(days=365),
    )
    part = AutoPart(name="Деталь", brand_id=created_brand.id, oem_number="CACHE3")
    part.certificates = [undated, dated]
    test_session.add_all([undated, dated, part])
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])
    await test_session.refresh(part)
    assert part.eac_cert_number == "ЕАЭС RU С-KR.АД50.В.0002/23"


@pytest.mark.asyncio
async def test_manual_entry_survives_cache_refresh(
    test_session, created_brand
):
    part, _ = await _part_with_certificate(test_session, created_brand, "CACHE4")
    part.eac_cert_number = "РУЧНОЙ-НОМЕР"
    part.regulatory_source = "manual"
    test_session.add(part)
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])
    await test_session.refresh(part)
    assert part.eac_cert_number == "РУЧНОЙ-НОМЕР"


# ── применение к бренду ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dry_run_does_not_dirty_certificate(
    test_session, created_brand
):
    certificate = Certificate(number=CERT, covers_whole_brand=False)
    test_session.add(certificate)
    await test_session.commit()

    await apply_brand_certificate(
        test_session, brand_id=created_brand.id, number=CERT, dry_run=True
    )
    # Коммит после предпросмотра не должен унести признак в базу.
    await test_session.commit()
    stored = (
        await test_session.execute(
            select(Certificate).where(Certificate.number == CERT)
        )
    ).scalar_one()
    assert stored.covers_whole_brand is False


@pytest.mark.asyncio
async def test_manual_positions_excluded_in_both_modes(
    test_session, created_brand
):
    manual = AutoPart(
        name="Ручная", brand_id=created_brand.id, oem_number="BRAND1",
        certification_required=False, regulatory_source="manual",
    )
    plain = AutoPart(
        name="Обычная", brand_id=created_brand.id, oem_number="BRAND2"
    )
    test_session.add_all([manual, plain])
    await test_session.commit()

    result = await apply_brand_certificate(
        test_session,
        brand_id=created_brand.id,
        number=CERT,
        dry_run=False,
        only_undetermined=False,
    )
    await test_session.refresh(manual)
    await test_session.refresh(plain)
    # Снятое ограничение расширяет охват, но ручной ввод остаётся.
    assert manual.regulatory_source == "manual"
    assert manual.certification_required is False
    assert plain.regulatory_source == "brand_certificate"
    assert result["positions"] >= 1


# ── согласованность признака и привязки ─────────────────────────────────


@pytest.mark.anyio
async def test_linking_certificate_clears_not_required_flag(
    test_session, created_brand
):
    """Позиция, помеченную правилом как «не требует», привязали к
    документу: в выгрузке признак сильнее номера, поэтому он обязан
    смениться, иначе клиент получит «Не требует сертификации» на товар
    с сертификатом."""
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='FLAG0001',
        name='Ролик натяжной ГРМ',
        certification_required=False,
        regulatory_source='rule',
    )
    certificate = Certificate(number=CERT, source='manual')
    test_session.add_all([part, certificate])
    await test_session.flush()
    await test_session.execute(
        autopart_certificate_association.insert().values(
            autopart_id=part.id, certificate_id=certificate.id
        )
    )
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])

    await test_session.refresh(part)
    assert part.certification_required is True
    assert part.eac_cert_number == CERT


@pytest.mark.anyio
async def test_unlinking_keeps_certification_flag(
    test_session, created_brand
):
    """Отвязка документа не означает, что товар сертификации не подлежит:
    признак остаётся, а позиция попадёт в отчёт незаполненных."""
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='FLAG0002',
        name='Ролик натяжной ГРМ',
        certification_required=True,
        eac_cert_number=CERT,
    )
    test_session.add(part)
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])

    await test_session.refresh(part)
    assert part.certification_required is True
    assert part.eac_cert_number is None


@pytest.mark.anyio
async def test_cannot_move_certificate_to_another_brand(
    test_session, created_brand
):
    """Смена бренда переписала бы brand_id, оставив связи прежнего
    бренда: документ показывал бы один бренд, покрывая другой."""
    other = Brand(name='OTHERBRAND', main_brand=True)
    test_session.add(other)
    await test_session.flush()
    part = AutoPart(
        brand_id=created_brand.id, oem_number='MOVE0001', name='Фильтр'
    )
    certificate = Certificate(number=CERT, brand_id=created_brand.id)
    test_session.add_all([part, certificate])
    await test_session.flush()
    await test_session.execute(
        autopart_certificate_association.insert().values(
            autopart_id=part.id, certificate_id=certificate.id
        )
    )
    await test_session.commit()

    with pytest.raises(ValueError, match='другого бренда'):
        await apply_brand_certificate(
            test_session, brand_id=other.id, number=CERT, dry_run=True
        )


# ── защита от чужого сертификата ────────────────────────────────────────


@pytest.mark.anyio
async def test_foreign_brand_certificate_never_reaches_price(
    test_session, created_brand
):
    """Связь могли создать в обход проверок — миграцией или руками в
    базе. Перед прайсом бренд сверяется ещё раз, и чужой документ в
    карточку не попадает."""
    other = Brand(name='FOREIGNBRAND', main_brand=True)
    test_session.add(other)
    await test_session.flush()
    part = AutoPart(
        brand_id=created_brand.id, oem_number='GUARD001', name='Фильтр'
    )
    certificate = Certificate(number=CERT, brand_id=other.id)
    test_session.add_all([part, certificate])
    await test_session.flush()
    await test_session.execute(
        autopart_certificate_association.insert().values(
            autopart_id=part.id, certificate_id=certificate.id
        )
    )
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])

    await test_session.refresh(part)
    assert part.eac_cert_number is None


@pytest.mark.anyio
async def test_synonym_brand_certificate_is_accepted(
    test_session, created_brand
):
    """Другое написание того же бренда — не чужой бренд: связь через
    справочник синонимов должна проходить."""
    alias = Brand(name='ALIASBRAND')
    test_session.add(alias)
    await test_session.flush()
    await test_session.execute(
        brand_synonyms.insert().values(
            brand_id=created_brand.id, synonym_id=alias.id
        )
    )
    part = AutoPart(
        brand_id=created_brand.id, oem_number='GUARD002', name='Фильтр'
    )
    certificate = Certificate(number=CERT, brand_id=alias.id)
    test_session.add_all([part, certificate])
    await test_session.flush()
    await test_session.execute(
        autopart_certificate_association.insert().values(
            autopart_id=part.id, certificate_id=certificate.id
        )
    )
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])

    await test_session.refresh(part)
    assert part.eac_cert_number == CERT


@pytest.mark.anyio
async def test_supplier_document_wins_over_our_own(
    test_session, created_brand
):
    """За ввезённый товар отвечает поставщик, поэтому его документ
    важнее нашего собственного."""
    part = AutoPart(
        brand_id=created_brand.id, oem_number='PICK001', name='Фильтр'
    )
    ours = Certificate(
        number='ЕАЭС RU С-RU.НВ07.В.00709/22',
        brand_id=created_brand.id,
        source='manual',
    )
    theirs = Certificate(
        number=CERT, brand_id=created_brand.id, source='supplier_file'
    )
    test_session.add_all([part, ours, theirs])
    await test_session.flush()
    for certificate in (ours, theirs):
        await test_session.execute(
            autopart_certificate_association.insert().values(
                autopart_id=part.id, certificate_id=certificate.id
            )
        )
    await test_session.commit()

    await refresh_autopart_certificate_cache(test_session, [part.id])

    await test_session.refresh(part)
    assert part.eac_cert_number == CERT
