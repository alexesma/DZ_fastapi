"""Автозаполнение корректировки поставщика (УКД/КСЧФ) из XML Диадока.

Находит исходное поступление по номеру и дате исходного УПД, заполняет
строки «до/после» и помечает расхождения. Остатки не меняются: применяет
корректировку только администратор через post_correction.
"""

from __future__ import annotations

import os
import re
from datetime import date
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    SupplierReceipt,
    SupplierReceiptCorrection,
    SupplierReceiptCorrectionItem,
)
from dz_fastapi.services.supplier_document_dedup import normalize_document_number
from dz_fastapi.services.upd_xml import CorrectionDocument, parse_correction_xml

DATE_TOLERANCE_DAYS = 45
_ALNUM_RE = re.compile(r"[^0-9A-Za-zА-Яа-яЁё]+")


def _oem_key(value: object) -> str:
    return _ALNUM_RE.sub("", str(value or "")).upper()


async def find_original_receipts(
    session: AsyncSession,
    *,
    provider_id: int,
    original_number: Optional[str],
    original_date: Optional[date],
) -> list[SupplierReceipt]:
    """Поступления поставщика с тем же номером (серийная часть) и близкой датой."""
    wanted = normalize_document_number(original_number)
    if not wanted:
        return []
    rows = (
        (
            await session.execute(
                select(SupplierReceipt)
                .options(selectinload(SupplierReceipt.items))
                .where(
                    SupplierReceipt.provider_id == provider_id,
                    SupplierReceipt.document_number.is_not(None),
                )
                .order_by(SupplierReceipt.id.desc())
            )
        )
        .scalars()
        .all()
    )
    found = []
    for receipt in rows:
        if normalize_document_number(receipt.document_number) != wanted:
            continue
        if original_date and receipt.document_date:
            if abs((receipt.document_date - original_date).days) > DATE_TOLERANCE_DAYS:
                continue
        found.append(receipt)
    found.sort(
        key=lambda r: (
            0 if original_date and r.document_date == original_date else 1,
            -r.id,
        )
    )
    return found


def _match_rows(
    doc: CorrectionDocument, receipt: SupplierReceipt
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_oem: dict[str, list] = {}
    for item in receipt.items:
        by_oem.setdefault(_oem_key(item.oem_number), []).append(item)
    ordered = sorted(receipt.items, key=lambda i: i.id)
    matched, unmatched, used = [], [], set()
    for row in doc.rows:
        candidates = [
            i for i in by_oem.get(_oem_key(row.article), []) if i.id not in used
        ]
        item = candidates[0] if candidates else None
        if item is None and not row.article and row.original_line_number:
            index = row.original_line_number - 1
            if 0 <= index < len(ordered) and ordered[index].id not in used:
                item = ordered[index]
        info = {
            "article": row.article,
            "name": row.name,
            "quantity_before": row.quantity_before,
            "quantity_after": row.quantity_after,
            "price_before": row.price_before,
            "price_after": row.price_after,
            "total_delta": row.total_delta,
        }
        if item is None:
            unmatched.append(info)
            continue
        used.add(item.id)
        matched.append({**info, "receipt_item_id": int(item.id),
                        "received_quantity": int(item.received_quantity or 0)})
    return matched, unmatched


async def fill_correction_from_document(
    session: AsyncSession,
    *,
    correction: SupplierReceiptCorrection,
    document: DiadocIncomingDocument,
) -> dict[str, Any]:
    """Заполняет correction и возвращает сводку (кладётся в parsed_details)."""
    path = document.local_file_path
    payload = None
    if path and os.path.exists(os.path.abspath(path)):
        with open(os.path.abspath(path), "rb") as fh:
            payload = fh.read()
    doc = parse_correction_xml(payload) if payload else None
    summary: dict[str, Any] = {"correction_id": int(correction.id)}
    if doc is None:
        summary["state"] = "not_parsed"
        summary["hint"] = "XML корректировки не разобран: заполните строки вручную"
        return summary
    summary.update(
        original_number=doc.original_number,
        original_date=doc.original_date.isoformat() if doc.original_date else None,
        total_delta=doc.total_delta,
    )
    rows_delta = [r.total_delta for r in doc.rows if r.total_delta is not None]
    if doc.total_delta is not None and rows_delta:
        if abs(sum(rows_delta) - doc.total_delta) > 0.01:
            summary["total_mismatch"] = {
                "rows": round(sum(rows_delta), 2), "document": doc.total_delta,
            }
    receipts = await find_original_receipts(
        session,
        provider_id=int(correction.provider_id),
        original_number=doc.original_number,
        original_date=doc.original_date,
    )
    if len(receipts) != 1:
        summary["state"] = "receipt_not_found" if not receipts else "receipt_ambiguous"
        summary["candidates"] = [
            {"id": int(r.id), "number": r.document_number,
             "date": r.document_date.isoformat() if r.document_date else None,
             "posted": r.posted_at is not None}
            for r in receipts[:10]
        ]
        summary["hint"] = (
            "Исходное поступление не найдено — привяжите вручную"
            if not receipts
            else "Найдено несколько поступлений — выберите нужное вручную"
        )
        return summary
    receipt = receipts[0]
    matched, unmatched = _match_rows(doc, receipt)
    correction.receipt_id = receipt.id
    correction.items.clear()
    await session.flush()
    for row in matched:
        correction.items.append(
            SupplierReceiptCorrectionItem(
                receipt_item_id=row["receipt_item_id"],
                quantity_before=row["received_quantity"],
                quantity_after=int(row["quantity_after"] or 0),
            )
        )
    note = [
        f"КСЧФ №{doc.number or '—'} к документу №{doc.original_number or '—'} "
        f"от {doc.original_date or '—'} (Диадок)."
    ]
    if unmatched:
        note.append(f"Строк без пары в поступлении: {len(unmatched)}.")
    if summary.get("total_mismatch"):
        note.append("Итог корректировки не сходится с суммой строк.")
    if receipt.posted_at is None:
        note.append("Исходное поступление не проведено: измените его строки.")
    correction.comment = " ".join(note)
    summary.update(
        state="matched",
        receipt_id=int(receipt.id),
        receipt_posted=receipt.posted_at is not None,
        matched=matched,
        unmatched=unmatched,
        hint="Проверьте строки и подтвердите проведение корректировки",
    )
    return summary
