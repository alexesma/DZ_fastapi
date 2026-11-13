"""Сканирование кодов маркировки при пересчёте поступления.

Коды в поступлении берутся из УПД поставщика. Чтобы не принимать их на
веру, кладовщик сканирует DataMatrix каждой штуки, а мы сверяем скан с
документом: код из документа — «подтверждён», кода в документе нет —
«лишний», код из документа, который так и не отсканировали, — «не найден».
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.autopart import ScannedCodeAlias
from dz_fastapi.models.inventory import ReceiptMarkingScan, ReceiptScanCount
from dz_fastapi.models.partner import SupplierReceipt
from dz_fastapi.services.inventory_stock import resolve_receipt_item_autopart_id
from dz_fastapi.services.marking_aggregate import (
    AggregateError,
    expand_aggregate,
    gis_mt_client_or_none,
    parse_transport_code,
)
from dz_fastapi.services.marking_code_parse import parse_marking_code
from dz_fastapi.services.marking_codes import normalize_marking_codes

SCAN_CONFIRMED = "confirmed"
SCAN_EXTRA = "extra"


class NotMarkingCode(ValueError):
    """Скан не является кодом маркировки."""


def _document_codes(receipt: SupplierReceipt) -> dict[str, Any]:
    """Код из документа → строка поступления (первая, если код повторяется)."""
    codes: dict[str, Any] = {}
    for item in receipt.items or []:
        for code in normalize_marking_codes(item.marking_codes):
            codes.setdefault(code, item)
    return codes


async def _scans(
    session: AsyncSession, receipt_id: int
) -> list[ReceiptMarkingScan]:
    return list(
        (
            await session.execute(
                select(ReceiptMarkingScan)
                .where(ReceiptMarkingScan.receipt_id == receipt_id)
                .order_by(ReceiptMarkingScan.id)
            )
        )
        .scalars()
        .all()
    )


async def marking_scan_state(
    session: AsyncSession, receipt: SupplierReceipt
) -> dict[str, Any]:
    """Состояние сверки кодов: по строкам и по документу в целом."""
    document = _document_codes(receipt)
    scans = await _scans(session, receipt.id)
    scanned = {scan.code for scan in scans}
    by_item: dict[int, dict[str, Any]] = {}
    for item in receipt.items or []:
        codes = [
            code
            for code in normalize_marking_codes(item.marking_codes)
            if document.get(code) is item
        ]
        by_item[item.id] = {
            "expected": len(codes),
            "confirmed": sum(1 for code in codes if code in scanned),
            "missing_codes": [code for code in codes if code not in scanned],
        }
    extra = [
        {
            "id": scan.id,
            "code": scan.code,
            "autopart_id": scan.autopart_id,
            "scanned_at": scan.scanned_at,
        }
        for scan in scans
        if scan.code not in document
    ]
    expected = len(document)
    confirmed = sum(row["confirmed"] for row in by_item.values())
    return {
        "document_has_codes": bool(document),
        "started": bool(scans),
        "expected": expected,
        "confirmed": confirmed,
        "missing": expected - confirmed,
        "extra": extra,
        "by_item": by_item,
    }


async def record_marking_scan(
    session: AsyncSession,
    receipt: SupplierReceipt,
    raw: object,
    *,
    user_id: Optional[int] = None,
) -> dict[str, Any]:
    """Записывает скан кода и возвращает его статус. Коммит — на вызывающем."""
    parsed = parse_marking_code(raw)
    if parsed is None:
        raise NotMarkingCode("Это не код маркировки")
    document = _document_codes(receipt)
    item = document.get(parsed.ki)
    existing = (
        await session.execute(
            select(ReceiptMarkingScan).where(
                ReceiptMarkingScan.receipt_id == receipt.id,
                ReceiptMarkingScan.code == parsed.ki,
            )
        )
    ).scalar_one_or_none()
    if item is not None:
        autopart_id = await resolve_receipt_item_autopart_id(session, item)
    else:
        autopart_id = (
            await session.execute(
                select(ScannedCodeAlias.autopart_id).where(
                    ScannedCodeAlias.code == f"GTIN:{parsed.gtin}"
                )
            )
        ).scalar_one_or_none()
    duplicate = existing is not None
    if existing is None:
        full_code = str(raw).strip() if parsed.has_crypto_tail else None
        existing = ReceiptMarkingScan(
            receipt_id=receipt.id,
            receipt_item_id=item.id if item is not None else None,
            autopart_id=autopart_id,
            code=parsed.ki,
            full_code=full_code,
            scanned_by_user_id=user_id,
        )
        session.add(existing)
        await session.flush()
    elif item is not None and existing.receipt_item_id != item.id:
        # Документ изменился после скана: код появился в строке.
        existing.receipt_item_id = item.id
        existing.autopart_id = autopart_id
    return {
        "id": existing.id,
        "code": parsed.ki,
        "status": SCAN_CONFIRMED if item is not None else SCAN_EXTRA,
        "duplicate": duplicate,
        "autopart_id": autopart_id,
        "receipt_item_id": item.id if item is not None else None,
        "document_has_codes": bool(document),
    }


async def record_aggregate_scan(
    session: AsyncSession,
    receipt: SupplierReceipt,
    raw: object,
    *,
    user_id: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """Скан упаковки: раскрывает её через ГИС МТ и засчитывает каждую штуку.

    Возвращает ``None``, если это не упаковка и скан надо обработать как
    обычный код. Подтверждённые штуки сразу считаются принятыми.
    """
    transport = parse_transport_code(raw)
    code = transport
    if code is None:
        parsed = parse_marking_code(raw)
        document = _document_codes(receipt)
        # Групповая упаковка выглядит как обычный код, но в документе её нет.
        if parsed is None or not document or parsed.ki in document:
            return None
        if await gis_mt_client_or_none(session) is None:
            return None
        code = parsed.ki
    try:
        units = await expand_aggregate(session, code)
    except AggregateError:
        if transport is None:
            return None
        raise
    if not units:
        if transport is None:
            return None
        raise AggregateError("ГИС МТ не знает вложений этой упаковки")

    confirmed = extra = duplicates = 0
    counted: dict[int, int] = {}
    for unit in units:
        try:
            result = await record_marking_scan(
                session, receipt, unit, user_id=user_id
            )
        except NotMarkingCode:
            continue
        if result["duplicate"]:
            duplicates += 1
        elif result["status"] == SCAN_CONFIRMED:
            confirmed += 1
            item_id = result["receipt_item_id"]
            counted[item_id] = counted.get(item_id, 0) + 1
        else:
            extra += 1
    if counted:
        rows = {
            row.receipt_item_id: row
            for row in (
                await session.execute(
                    select(ReceiptScanCount)
                    .where(ReceiptScanCount.receipt_item_id.in_(counted))
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        }
        for item_id, quantity in counted.items():
            row = rows.get(item_id)
            if row is None:
                row = ReceiptScanCount(
                    receipt_item_id=item_id, accepted_qty=0, placed_qty=0
                )
                session.add(row)
            row.accepted_qty = int(row.accepted_qty or 0) + quantity
        await session.flush()
    return {
        "aggregate": True,
        "code": code,
        "units": len(units),
        "confirmed": confirmed,
        "extra": extra,
        "duplicates": duplicates,
    }
