"""ОКПД 2 выводится из ТН ВЭД по загруженной таблице соответствия.

Поставщики ни ТН ВЭД, ни ОКПД 2 не передают: в семи файлах эти колонки
пусты во всех 405 690 строках. Заполнять приходится самим, и ОКПД 2
берётся из ТН ВЭД — но только по официальной таблице и только там, где
соответствие однозначно: оно один ко многим, а выбранный за человека
код уедет клиенту как достоверный.
"""
import pytest
from sqlalchemy import select

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.certificate import TnvedOkpd2Match
from dz_fastapi.services.regulatory import (
    apply_okpd2_from_tnved,
    import_tnved_okpd2_table,
    normalize_code,
    parse_tnved_okpd2_file,
)

HEADER = 'ТН ВЭД;ОКПД 2'


def _csv(*lines: str) -> bytes:
    return '\r\n'.join((HEADER, *lines)).encode('utf-8')


# ── разбор файла ────────────────────────────────────────────────────────


def test_parses_codes_ignoring_separators():
    """В таблицах один и тот же код пишут «8708 80 100 0» и
    «8708801000» — сравнивать нужно по цифрам."""
    rows = parse_tnved_okpd2_file(_csv('8708 80 100 0;29.32.30.390'))
    assert rows == [
        {'tnved_prefix': '8708801000', 'okpd2_code': '29.32.30.390'}
    ]


def test_rejects_file_without_required_columns():
    with pytest.raises(ValueError, match='ТН ВЭД'):
        parse_tnved_okpd2_file(b'Code;Value\r\n1;2')


def test_normalize_code_keeps_only_digits():
    assert normalize_code('8708 80 100 0') == '8708801000'
    assert normalize_code('29.32.30.390') == '293230390'
    assert normalize_code(None) == ''


# ── проставление ────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_fills_okpd2_when_match_is_unique(test_session, created_brand):
    await import_tnved_okpd2_table(
        test_session,
        parse_tnved_okpd2_file(_csv('8708801000;29.32.30.390')),
        dry_run=False,
    )
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='OKPD001',
        name='Сайлентблок',
        tnved_code='8708801000',
    )
    test_session.add(part)
    await test_session.commit()

    result = await apply_okpd2_from_tnved(test_session, dry_run=False)
    assert result['updated'] == 1

    await test_session.refresh(part)
    assert part.okpd2_code == '29.32.30.390'


@pytest.mark.anyio
async def test_ambiguous_match_is_left_to_human(test_session, created_brand):
    """Одному ТН ВЭД отвечают два ОКПД 2 — выбирать за человека нельзя."""
    await import_tnved_okpd2_table(
        test_session,
        parse_tnved_okpd2_file(
            _csv('8708801000;29.32.30.390', '8708801000;29.32.30.230')
        ),
        dry_run=False,
    )
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='OKPD002',
        name='Сайлентблок',
        tnved_code='8708801000',
    )
    test_session.add(part)
    await test_session.commit()

    result = await apply_okpd2_from_tnved(test_session, dry_run=False)
    assert result['ambiguous'] == 1
    assert result['updated'] == 0

    await test_session.refresh(part)
    assert part.okpd2_code is None


@pytest.mark.anyio
async def test_longest_prefix_wins(test_session, created_brand):
    """Укрупнённый код в таблице не должен перебивать точный."""
    await import_tnved_okpd2_table(
        test_session,
        parse_tnved_okpd2_file(
            _csv('8708;29.32.30.000', '8708801000;29.32.30.390')
        ),
        dry_run=False,
    )
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='OKPD003',
        name='Сайлентблок',
        tnved_code='8708801000',
    )
    test_session.add(part)
    await test_session.commit()

    await apply_okpd2_from_tnved(test_session, dry_run=False)

    await test_session.refresh(part)
    assert part.okpd2_code == '29.32.30.390'


@pytest.mark.anyio
async def test_dry_run_writes_nothing(test_session, created_brand):
    await import_tnved_okpd2_table(
        test_session,
        parse_tnved_okpd2_file(_csv('8708801000;29.32.30.390')),
        dry_run=False,
    )
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='OKPD004',
        name='Сайлентблок',
        tnved_code='8708801000',
    )
    test_session.add(part)
    await test_session.commit()

    result = await apply_okpd2_from_tnved(test_session, dry_run=True)
    assert result['updated'] == 1

    await test_session.refresh(part)
    assert part.okpd2_code is None


@pytest.mark.anyio
async def test_without_table_nothing_happens(test_session, created_brand):
    """Таблицу не загрузили — проставлять не из чего, и позиции даже не
    перебираются."""
    part = AutoPart(
        brand_id=created_brand.id,
        oem_number='OKPD005',
        name='Сайлентблок',
        tnved_code='8708801000',
    )
    test_session.add(part)
    await test_session.commit()

    result = await apply_okpd2_from_tnved(test_session, dry_run=False)
    assert result == {
        'table_rows': 0,
        'positions': 0,
        'updated': 0,
        'ambiguous': 0,
        'no_match': 0,
    }


@pytest.mark.anyio
async def test_import_is_idempotent(test_session):
    rows = parse_tnved_okpd2_file(_csv('8708801000;29.32.30.390'))
    first = await import_tnved_okpd2_table(test_session, rows, dry_run=False)
    second = await import_tnved_okpd2_table(test_session, rows, dry_run=False)
    assert first['created'] == 1
    assert second['created'] == 0
    assert second['existing'] == 1

    stored = (
        await test_session.execute(select(TnvedOkpd2Match))
    ).scalars().all()
    assert len(stored) == 1
