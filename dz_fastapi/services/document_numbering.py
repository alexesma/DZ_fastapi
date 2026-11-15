"""Собственная сквозная нумерация документов.

УНФ не присваивает номера документам, пришедшим по обмену: она хранит тот
номер, который передали мы. Поэтому бухгалтерский порядковый номер выдаёт
DragonZap — при проведении, без пропусков, с началом заново каждый год.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import DocumentNumbering

KIND_SHIPMENT = "shipment"
# Серии ведутся по виду документа: новому виду (например, корректировкам
# реализации) достаточно добавить запись сюда и вызвать next_document_number.
KINDS = {KIND_SHIPMENT: "Реализации"}
DEFAULT_PREFIX = {KIND_SHIPMENT: "ДЗ-"}
DEFAULT_DIGITS = 6
# Поле номера документа в УНФ вмещает 11 символов.
MAX_NUMBER_LENGTH = 11


def format_number(prefix: str, digits: int, value: int) -> str:
    return f"{prefix}{int(value):0{int(digits)}d}"


def validate_format(prefix: str, digits: int) -> None:
    if not 3 <= int(digits) <= 9:
        raise ValueError("Количество цифр в номере — от 3 до 9")
    if len(prefix) + int(digits) > MAX_NUMBER_LENGTH:
        raise ValueError(
            f"Номер длиннее {MAX_NUMBER_LENGTH} символов не поместится в документ УНФ"
        )


def fits_unf(number: object) -> bool:
    text = str(number or "").strip()
    return 0 < len(text) <= MAX_NUMBER_LENGTH


async def _row(
    session: AsyncSession, kind: str, *, lock: bool = False
) -> Optional[DocumentNumbering]:
    query = select(DocumentNumbering).where(DocumentNumbering.kind == kind)
    if lock:
        query = query.with_for_update()
    return (await session.execute(query)).scalar_one_or_none()


def numbering_data(kind: str, row: Optional[DocumentNumbering]) -> dict[str, Any]:
    prefix = row.prefix if row is not None else DEFAULT_PREFIX[kind]
    digits = row.digits if row is not None else DEFAULT_DIGITS
    year = now_moscow().year
    last = int(row.last_value or 0) if row is not None and row.year == year else 0
    return {
        "kind": kind,
        "title": KINDS[kind],
        "enabled": bool(row.enabled) if row is not None else False,
        "prefix": prefix,
        "digits": digits,
        "year": year,
        "last_value": last,
        "next_number": format_number(prefix, digits, last + 1),
    }


async def get_numbering(session: AsyncSession) -> list[dict[str, Any]]:
    return [numbering_data(kind, await _row(session, kind)) for kind in KINDS]


async def update_numbering(
    session: AsyncSession,
    kind: str,
    *,
    enabled: bool,
    prefix: str,
    digits: int,
    last_value: Optional[int] = None,
) -> dict[str, Any]:
    """Настройка серии. ``last_value`` — продолжить нумерацию с этого номера."""
    if kind not in KINDS:
        raise ValueError("Неизвестный вид документа")
    prefix = str(prefix or "").strip()
    validate_format(prefix, digits)
    row = await _row(session, kind, lock=True)
    year = now_moscow().year
    if row is None:
        row = DocumentNumbering(kind=kind, year=year, last_value=0)
        session.add(row)
    row.enabled = bool(enabled)
    row.prefix = prefix
    row.digits = int(digits)
    if last_value is not None:
        if int(last_value) < 0:
            raise ValueError("Последний номер не может быть отрицательным")
        row.year = year
        row.last_value = int(last_value)
    await session.flush()
    return numbering_data(kind, row)


async def next_document_number(session: AsyncSession, kind: str) -> Optional[str]:
    """Следующий номер серии или ``None``, если своя нумерация выключена.

    Счётчик меняется в транзакции вызывающего: если проведение откатится,
    номер не пропадёт — пропусков в серии не будет.
    """
    row = await _row(session, kind, lock=True)
    if row is None or not row.enabled:
        return None
    year = now_moscow().year
    if row.year != year:
        row.year = year
        row.last_value = 0
    row.last_value = int(row.last_value or 0) + 1
    await session.flush()
    return format_number(row.prefix, row.digits, row.last_value)
