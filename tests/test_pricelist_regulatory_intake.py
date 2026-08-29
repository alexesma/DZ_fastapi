"""Реквизиты приезжают вместе с обычным прайсом поставщика.

До этого приём читал ровно семь колонок — артикул, бренд, наименование,
кратность, количество, цену и начальную строку, — а ТН ВЭД, ОКПД 2, ЧЗ,
номер сертификата и ссылку отбрасывал. Данные попадали в систему только
разовой ручной загрузкой и устаревали вместе с ассортиментом.

Отдельно закреплено, что сбой в реквизитах не роняет приём прайса:
загрузка цен и остатков — основной путь всей системы.
"""
import io

import pandas as pd
import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.services.process import (
    _apply_pricelist_regulatory,
    _collect_regulatory_rows,
    process_provider_pricelist,
)

# Колонки: 0 артикул, 1 бренд, 2 наименование, 3 кол-во, 4 цена,
# 5 ТН ВЭД, 6 ОКПД 2, 7 ЧЗ, 8 сертификат, 9 ссылка.
REGULATORY_COLS = {
    "tnved_code": 5,
    "okpd2_code": 6,
    "honest_sign": 7,
    "eac_cert_number": 8,
    "eac_cert_url": 9,
}
CERT = 'ЕАЭС RU С-CN.НА96.В.02398/22'
CERT_URL = 'https://pub.fsa.gov.ru/rss/certificate/view/3246778/baseInfo'


def _frame(brand: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            ['Артикул', 'Бренд', 'Наименование', 'Кол-во', 'Цена',
             'ТН ВЭД', 'ОКПД 2', 'Подключен к ЧЗ',
             'Номер сертификата ЕАС', 'Ссылка ФГИС'],
            ['INTAKE001', brand, 'Фильтр масляный', '10', '250',
             '8708801000', '29.32.30.390', '', CERT, CERT_URL],
        ]
    )


def _excel_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_excel(buffer, index=False, header=False)
    return buffer.getvalue()


# ── чтение колонок ──────────────────────────────────────────────────────


def test_regulatory_columns_read_from_pricelist_frame():
    frame = _frame('TESTBRAND').iloc[1:]
    rows = _collect_regulatory_rows(frame, 1, 0, 2, REGULATORY_COLS)
    assert len(rows) == 1
    assert rows[0]['brand'] == 'TESTBRAND'
    assert rows[0]['article'] == 'INTAKE001'
    assert rows[0]['tnved_code'] == '8708801000'
    assert rows[0]['eac_cert_number'] == CERT
    assert rows[0]['eac_cert_url'] == CERT_URL


def test_no_regulatory_columns_configured_reads_nothing():
    """У поставщика этих колонок нет — приём не меняется вовсе."""
    frame = _frame('TESTBRAND').iloc[1:]
    assert _collect_regulatory_rows(frame, 1, 0, 2, None) == []
    assert _collect_regulatory_rows(frame, 1, 0, 2, {}) == []


def test_without_brand_column_regulatory_is_skipped():
    """Без бренда позицию не с чем сопоставить, а угадывать нельзя:
    приписали бы чужой сертификат."""
    frame = _frame('TESTBRAND').iloc[1:]
    assert _collect_regulatory_rows(frame, None, 0, 2, REGULATORY_COLS) == []


# ── сквозной приём прайса ───────────────────────────────────────────────


@pytest.mark.anyio
async def test_pricelist_intake_writes_regulatory(
    test_session, created_providers, created_pricelist_config, created_brand
):
    config = created_pricelist_config
    config.tnved_col = REGULATORY_COLS['tnved_code']
    config.okpd2_col = REGULATORY_COLS['okpd2_code']
    config.honest_sign_col = REGULATORY_COLS['honest_sign']
    config.eac_cert_col = REGULATORY_COLS['eac_cert_number']
    config.eac_cert_url_col = REGULATORY_COLS['eac_cert_url']
    test_session.add(config)
    await test_session.commit()

    await process_provider_pricelist(
        provider=created_providers[0],
        file_content=_excel_bytes(_frame(created_brand.name)),
        file_extension='xlsx',
        provider_list_conf=config,
        use_stored_params=True,
        start_row=None,
        oem_col=None,
        brand_col=None,
        name_col=None,
        multiplicity_col=None,
        qty_col=None,
        price_col=None,
        session=test_session,
        enforce_anomaly_guard=False,
    )

    part = (
        await test_session.execute(
            select(AutoPart).where(AutoPart.oem_number == 'INTAKE001')
        )
    ).scalar_one()
    assert part.tnved_code == '8708801000'
    assert part.okpd2_code == '29.32.30.390'
    assert part.eac_cert_number == CERT
    assert part.eac_cert_url == CERT_URL


@pytest.mark.anyio
async def test_regulatory_failure_does_not_break_intake(
    test_session, monkeypatch
):
    """Сбой в реквизитах не должен ронять приём прайса."""
    async def explode(*args, **kwargs):
        raise RuntimeError('реестр упал')

    monkeypatch.setattr(
        'dz_fastapi.services.process.import_supplier_regulatory', explode
    )
    result = await _apply_pricelist_regulatory(
        test_session, [{'brand': 'X', 'article': 'Y'}]
    )
    assert result == {'rows': 1, 'failed': True}
