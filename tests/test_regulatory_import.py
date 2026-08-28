"""Импорт обязательных реквизитов и распространение сертификатов.

Ограничители распространения покрыты тестами намеренно: без них перенос
сертификата на однобрендовые позиции даёт кратный переброс (на реальных
данных — 3 позиции с сертификатом превращались в 10 872 записи).
"""
import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation
from dz_fastapi.services.regulatory import (
    _split_certificate,
    import_supplier_regulatory,
    parse_supplier_regulatory_file,
    propagate_certificates_by_brand,
)

HEADER = (
    "Бренд;Артикул;Описание;ТНВЭД;ОКПД 2;"
    "Подключен к ЧЗ;Номер сертификата ЕАС;Ссылка ФГИС"
)


def _csv(*lines: str, encoding: str = "cp1251") -> bytes:
    return "\r\n".join((HEADER, *lines)).encode(encoding)


# ── разбор файла ────────────────────────────────────────────────────────


def test_parses_cp1251_and_maps_columns():
    content = _csv(
        "555;SB1392;Сайлентблок;8708801000;;;"
        "ЕАЭС RU С-JP.АД50.В.05948/23;https://pub.fsa.gov.ru/x"
    )
    rows, columns = parse_supplier_regulatory_file(content)
    assert set(columns) >= {"brand", "article", "eac_cert_number"}
    assert rows[0]["brand"] == "555"
    assert rows[0]["article"] == "SB1392"
    assert rows[0]["tnved_code"] == "8708801000"
    assert rows[0]["eac_cert_number"].startswith("ЕАЭС RU")


def test_parses_utf8_too():
    rows, _ = parse_supplier_regulatory_file(
        _csv("555;SB1392;Сайлентблок;;;;;", encoding="utf-8")
    )
    assert rows[0]["article"] == "SB1392"


def test_rows_without_brand_or_article_are_skipped():
    rows, _ = parse_supplier_regulatory_file(
        _csv(";SB1;Без бренда;;;;;", "555;;Без артикула;;;;;", "555;SB2;Ок;;;;;")
    )
    assert [row["article"] for row in rows] == ["SB2"]


def test_missing_required_column_raises():
    with pytest.raises(ValueError, match="обязательные колонки"):
        parse_supplier_regulatory_file("Цена;Количество\r\n1;2".encode("cp1251"))


# ── разбор колонки сертификата ──────────────────────────────────────────


def test_certificate_text_splits_into_flag_and_number():
    assert _split_certificate("ЕАЭС RU С-JP.АД50.В.05948/23") == (
        True,
        "ЕАЭС RU С-JP.АД50.В.05948/23",
    )
    assert _split_certificate("Не требует сертификации") == (False, None)
    assert _split_certificate("не требует сертификации") == (False, None)
    assert _split_certificate("") == (None, None)
    assert _split_certificate(None) == (None, None)


# ── импорт в карточки ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_import_fills_card_and_respects_manual(
    test_session, created_autopart, created_brand
):
    rows = [
        {
            "brand": created_brand.name,
            "article": created_autopart.oem_number,
            "name": "",
            "tnved_code": "8708801000",
            "okpd2_code": None,
            "honest_sign": None,
            "eac_cert_number": "ЕАЭС RU С-JP.АД50.В.05948/23",
            "eac_cert_url": "https://pub.fsa.gov.ru/x",
        }
    ]
    stats = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert stats["matched"] == 1
    assert stats["updated"] == 1

    await test_session.refresh(created_autopart)
    assert created_autopart.tnved_code == "8708801000"
    assert created_autopart.certification_required is True
    assert created_autopart.regulatory_source == "supplier_doc"

    # Ручной ввод чужой файл не перетирает.
    created_autopart.regulatory_source = "manual"
    created_autopart.tnved_code = "0000000000"
    test_session.add(created_autopart)
    await test_session.commit()

    rows[0]["tnved_code"] = "9999999999"
    stats = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert stats["skipped_manual"] == 1
    await test_session.refresh(created_autopart)
    assert created_autopart.tnved_code == "0000000000"


@pytest.mark.asyncio
async def test_dry_run_changes_nothing(
    test_session, created_autopart, created_brand
):
    rows = [
        {
            "brand": created_brand.name,
            "article": created_autopart.oem_number,
            "name": "",
            "tnved_code": "8708801000",
            "okpd2_code": None,
            "honest_sign": None,
            "eac_cert_number": None,
            "eac_cert_url": None,
        }
    ]
    stats = await import_supplier_regulatory(test_session, rows, dry_run=True)
    assert stats["updated"] == 1
    await test_session.refresh(created_autopart)
    assert created_autopart.tnved_code is None


# ── ограничители распространения ────────────────────────────────────────


async def _stock_part(session, brand, oem, pricelist, cert=None):
    part = AutoPart(name=f"Деталь {oem}", brand_id=brand.id, oem_number=oem)
    if cert:
        part.eac_cert_number = cert
        part.certification_required = True
    session.add(part)
    await session.flush()
    session.add(
        PriceListAutoPartAssociation(
            pricelist_id=pricelist.id,
            autopart_id=part.id,
            quantity=1,
            price=100,
        )
    )
    return part


@pytest.mark.asyncio
async def test_propagation_refuses_thin_evidence(
    test_session, created_brand, created_providers, created_pricelist_config
):
    provider = created_providers[0]
    pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
    )
    test_session.add(pricelist)
    await test_session.flush()

    cert = "ЕАЭС RU С-CN.НА96.В.02398/22"
    await _stock_part(test_session, created_brand, "EVID1", pricelist, cert)
    for index in range(30):
        await _stock_part(
            test_session, created_brand, f"BLANK{index}", pricelist
        )
    await test_session.commit()

    result = await propagate_certificates_by_brand(
        test_session, provider_id=provider.id, dry_run=False
    )
    # Одна позиция в основании — распространять нельзя.
    assert result["brands_thin_evidence"] == 1
    assert result["positions_updated"] == 0

    filled = (
        await test_session.execute(
            select(AutoPart).where(
                AutoPart.oem_number.like("BLANK%"),
                AutoPart.eac_cert_number.is_not(None),
            )
        )
    ).scalars().all()
    assert filled == []


@pytest.mark.asyncio
async def test_propagation_refuses_over_expansion(
    test_session, created_brand, created_providers, created_pricelist_config
):
    provider = created_providers[0]
    pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
    )
    test_session.add(pricelist)
    await test_session.flush()

    cert = "ЕАЭС RU С-CN.НА96.В.02398/22"
    for index in range(20):
        await _stock_part(
            test_session, created_brand, f"EV{index}", pricelist, cert
        )
    for index in range(500):
        await _stock_part(
            test_session, created_brand, f"BL{index}", pricelist
        )
    await test_session.commit()

    result = await propagate_certificates_by_brand(
        test_session, provider_id=provider.id, dry_run=True
    )
    # 20 оснований против 500 целей — расширение в 25 раз, отказ.
    assert result["brands_over_expansion"] == 1
    assert result["positions_updated"] == 0


@pytest.mark.asyncio
async def test_propagation_applies_within_ratio(
    test_session, created_brand, created_providers, created_pricelist_config
):
    provider = created_providers[0]
    pricelist = PriceList(
        provider_id=provider.id,
        provider_config_id=created_pricelist_config.id,
    )
    test_session.add(pricelist)
    await test_session.flush()

    cert = "ЕАЭС RU С-CN.НА96.В.02398/22"
    for index in range(25):
        await _stock_part(
            test_session, created_brand, f"OK{index}", pricelist, cert
        )
    for index in range(10):
        await _stock_part(
            test_session, created_brand, f"FILL{index}", pricelist
        )
    await test_session.commit()

    result = await propagate_certificates_by_brand(
        test_session, provider_id=provider.id, dry_run=False
    )
    assert result["brands_applied"] == 1
    assert result["positions_updated"] == 10

    updated = (
        await test_session.execute(
            select(AutoPart).where(AutoPart.oem_number.like("FILL%"))
        )
    ).scalars().all()
    assert all(part.eac_cert_number == cert for part in updated)
    assert all(part.regulatory_source == "brand_rule" for part in updated)
