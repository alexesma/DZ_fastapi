"""Импорт обязательных реквизитов и распространение сертификатов.

Ограничители распространения покрыты тестами намеренно: без них перенос
сертификата на однобрендовые позиции даёт кратный переброс (на реальных
данных — 3 позиции с сертификатом превращались в 10 872 записи).
"""
import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.brand import Brand, brand_synonyms
from dz_fastapi.models.nomenclature import HonestSignCategory
from dz_fastapi.models.partner import PriceList, PriceListAutoPartAssociation
from dz_fastapi.services.regulatory import (
    _split_certificate,
    chunked,
    import_supplier_regulatory,
    normalize_brand_key,
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


# ── сопоставление брендов и порционные запросы ──────────────────────────


@pytest.mark.parametrize(
    "written, catalogue",
    [
        ("Hyundai/Kia", "HYUNDAI-KIA"),
        ("Citroen/Peugeot", "CITROEN-PEUGEOT"),
        ("Master KiT", "MASTERKIT"),
        ("  febi  ", "FEBI"),
    ],
)
def test_brand_key_ignores_case_and_separators(written, catalogue):
    """Поставщики пишут бренд по-своему: «Hyundai/Kia» и «HYUNDAI-KIA» —
    один и тот же бренд каталога, иначе позиции остаются без реквизитов."""
    assert normalize_brand_key(written) == normalize_brand_key(catalogue)


def test_brand_key_keeps_different_brands_apart():
    assert normalize_brand_key('MANN') != normalize_brand_key('MANNOL')


def test_chunked_splits_by_size_and_keeps_order():
    """asyncpg не принимает больше 32 767 параметров: список для IN режем
    порциями, иначе импорт всех файлов падает на InterfaceError."""
    values = list(range(25))
    assert list(chunked(values, 10)) == [
        list(range(10)), list(range(10, 20)), list(range(20, 25))
    ]
    assert list(chunked([], 10)) == []
    assert list(chunked([1, 2], 10)) == [[1, 2]]


@pytest.mark.anyio
async def test_import_matches_part_when_brand_written_differently(
    test_session, created_brand
):
    """Бренд в файле написан иначе, чем в каталоге, — позиция всё равно
    должна получить реквизиты."""
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='BRANDKEY1',
        name='Фильтр масляный',
    )
    test_session.add(part)
    await test_session.commit()

    disguised = created_brand.name.lower().replace('-', ' / ')
    content = _csv(
        f'{disguised};BRANDKEY1;Фильтр масляный;8708;;;'
        f'ЕАЭС RU С-CN.НА96.В.02398/22;https://pub.fsa.gov.ru/y'
    )
    rows, _ = parse_supplier_regulatory_file(content)
    result = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert result['matched'] == 1

    await test_session.refresh(part)
    assert part.eac_cert_number == 'ЕАЭС RU С-CN.НА96.В.02398/22'


@pytest.mark.anyio
async def test_certificate_matches_part_through_brand_synonym(test_session):
    """Сертификат выписан на одно написание бренда, позиция заведена под
    другим: связь берётся из справочника синонимов, а не угадывается."""
    main = Brand(name='LUKOIL', main_brand=True)
    alias = Brand(name='ЛУКОЙЛ')
    test_session.add_all([main, alias])
    await test_session.flush()
    await test_session.execute(
        brand_synonyms.insert().values(
            brand_id=main.id, synonym_id=alias.id
        )
    )
    part = AutoPart(
        brand_id=main.id, oem_number='SYN0001', name='Масло моторное'
    )
    test_session.add(part)
    await test_session.commit()

    rows, _ = parse_supplier_regulatory_file(
        _csv(
            'ЛУКОЙЛ;SYN0001;Масло моторное;2710;;;'
            'ЕАЭС RU С-RU.АД50.В.05948/23;https://pub.fsa.gov.ru/z'
        )
    )
    result = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert result['matched'] == 1

    await test_session.refresh(part)
    assert part.eac_cert_number == 'ЕАЭС RU С-RU.АД50.В.05948/23'


@pytest.mark.anyio
async def test_unrelated_brands_do_not_match(test_session):
    """Без записи в синонимах разные написания остаются разными
    брендами — иначе позиции получат чужой сертификат."""
    ours = Brand(name='MANNOL', main_brand=True)
    test_session.add(ours)
    await test_session.flush()
    part = AutoPart(
        brand_id=ours.id, oem_number='SYN0002', name='Масло моторное'
    )
    test_session.add(part)
    await test_session.commit()

    rows, _ = parse_supplier_regulatory_file(
        _csv(
            'MANN;SYN0002;Масло моторное;2710;;;'
            'ЕАЭС RU С-RU.АД50.В.05949/23;https://pub.fsa.gov.ru/z'
        )
    )
    result = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert result['matched'] == 0

    await test_session.refresh(part)
    assert part.eac_cert_number is None


# ── Честный знак из прайса поставщика ───────────────────────────────────


@pytest.mark.anyio
async def test_honest_sign_category_name_is_linked(
    test_session, created_brand
):
    """В колонке «Подключен к ЧЗ» стоит название категории — связываем
    её со справочником, значение не теряется."""
    category = HonestSignCategory(name='Шины и покрышки')
    part = AutoPart(
        brand_id=created_brand.id, oem_number='HS0001', name='Шина'
    )
    test_session.add_all([category, part])
    await test_session.commit()

    rows, _ = parse_supplier_regulatory_file(
        _csv('TEST BRAND;HS0001;Шина;4011;;шины и покрышки;;')
    )
    result = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert result['honest_sign_linked'] == 1

    await test_session.refresh(part)
    assert part.honest_sign_category == 'Шины и покрышки'


@pytest.mark.anyio
async def test_honest_sign_flag_is_counted_not_guessed(
    test_session, created_brand
):
    """«Да» не называет категорию, и угадать её нельзя — считаем
    отдельно, чтобы значение не пропадало молча."""
    part = AutoPart(
        brand_id=created_brand.id, oem_number='HS0002', name='Фильтр'
    )
    test_session.add(part)
    await test_session.commit()

    rows, _ = parse_supplier_regulatory_file(
        _csv('TEST BRAND;HS0002;Фильтр;8421;;Да;;')
    )
    result = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert result['honest_sign_flag_only'] == 1
    assert result['honest_sign_linked'] == 0

    await test_session.refresh(part)
    assert part.honest_sign_category is None


@pytest.mark.anyio
async def test_unknown_honest_sign_value_is_reported(
    test_session, created_brand
):
    """Незнакомая категория попадает в отчёт, а не в карточку."""
    part = AutoPart(
        brand_id=created_brand.id, oem_number='HS0003', name='Фильтр'
    )
    test_session.add(part)
    await test_session.commit()

    rows, _ = parse_supplier_regulatory_file(
        _csv('TEST BRAND;HS0003;Фильтр;8421;;Неизвестная категория;;')
    )
    result = await import_supplier_regulatory(
        test_session, rows, dry_run=False
    )
    assert result['honest_sign_unknown'] == {'Неизвестная категория': 1}

    await test_session.refresh(part)
    assert part.honest_sign_category is None
