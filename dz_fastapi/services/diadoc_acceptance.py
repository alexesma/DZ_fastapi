"""Сверка входящего УПД с фактической приёмкой перед подписью.

Подпись титула покупателя — юридическое подтверждение приёмки, поэтому код
итога («принято без расхождений» / «с расхождениями») должен соответствовать
тому, что реально проведено в поступлении. Здесь сравниваются количества из
XML УПД с количествами проведённых поступлений и учитываются ошибки сверки
кодов маркировки.
"""

from __future__ import annotations

import os
import re
from collections import Counter, defaultdict
from typing import Any, Iterable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.inventory import ReceiptMarkingScan
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem
from dz_fastapi.services.marking_codes import autoparts_marked_by_category, normalize_marking_codes
from dz_fastapi.services.upd_xml import UpdXmlItem, looks_like_upd_xml, parse_upd_xml

TOTAL_CODE_ACCEPTED = "1"
TOTAL_CODE_WITH_DISCREPANCIES = "2"
TOTAL_CODE_NOT_ACCEPTED = "3"

_MAX_LINES = 50
_KEY_RE = re.compile(r"[^0-9A-ZА-ЯЁ]+")


def _key(value: object) -> str:
    return _KEY_RE.sub("", str(value or "").upper())


def _label(article: object, name: object) -> str:
    article_text = str(article or "").strip()
    name_text = str(name or "").strip()
    if article_text and name_text:
        return f"{article_text} — {name_text}"[:200]
    return (article_text or name_text or "без артикула")[:200]


def _group(rows: Iterable[tuple[object, object, int, list[str]]]) -> dict[str, dict[str, Any]]:
    """Складывает количества по артикулу; без артикула — по наименованию."""
    grouped: dict[str, dict[str, Any]] = {}
    for article, name, quantity, codes in rows:
        article_key = _key(article)
        key = f"a:{article_key}" if article_key else f"n:{_key(name)}"
        bucket = grouped.setdefault(
            key,
            {
                "label": _label(article, name),
                "name_key": _key(name),
                "quantity": 0,
                "codes": Counter(),
            },
        )
        bucket["codes"].update(normalize_marking_codes(codes))
        bucket["quantity"] += int(quantity or 0)
    return grouped


def _marking_errors(item: SupplierReceiptItem) -> int:
    check = item.marking_check or {}
    return sum(
        1
        for issue in check.get("issues") or []
        if isinstance(issue, dict) and issue.get("severity") == "error"
    )


def compare_upd_with_receipt(
    upd_items: Iterable[UpdXmlItem],
    receipt_items: Iterable[SupplierReceiptItem],
) -> list[dict[str, Any]]:
    """Список расхождений между строками УПД и проведённой приёмкой."""
    receipt_items = list(receipt_items)
    document = _group(
        (item.oem_number, item.name, int(item.quantity or 0), item.marking_codes)
        for item in upd_items
    )
    received = _group(
        (item.oem_number, item.autopart_name, int(item.received_quantity or 0), item.marking_codes)
        for item in receipt_items
    )
    # Артикул в поступлении мог быть исправлен при приёмке: такие строки
    # сопоставляем по наименованию, чтобы не показывать ложное расхождение.
    only_document = [key for key in document if key not in received]
    only_received = {
        received[key]["name_key"]: key
        for key in received
        if key not in document and received[key]["name_key"]
    }
    pairs: dict[str, str] = {key: key for key in document if key in received}
    for key in only_document:
        other = only_received.pop(document[key]["name_key"], None)
        if other is not None:
            pairs[key] = other

    lines: list[dict[str, Any]] = []
    matched_received = set(pairs.values())
    for key, doc_row in document.items():
        doc_qty = int(doc_row["quantity"])
        other_key = pairs.get(key)
        got_qty = int(received[other_key]["quantity"]) if other_key else 0
        expected = doc_row["codes"]
        actual = received[other_key]["codes"] if other_key else Counter()
        if expected != actual or (expected and sum(expected.values()) != doc_qty):
            lines.append(
                {
                    "kind": "marking",
                    "label": f"{doc_row['label']}: состав кодов УПД и поступления",
                    "document_quantity": sum(expected.values()),
                    "received_quantity": sum((actual & expected).values()),
                }
            )
        if got_qty == doc_qty:
            continue
        if got_qty == 0:
            kind = "missing"
        elif got_qty < doc_qty:
            kind = "shortage"
        else:
            kind = "surplus"
        lines.append(
            {
                "kind": kind,
                "label": doc_row["label"],
                "document_quantity": doc_qty,
                "received_quantity": got_qty,
            }
        )
    for key, row in received.items():
        if key in matched_received or int(row["quantity"]) == 0:
            continue
        lines.append(
            {
                "kind": "extra",
                "label": row["label"],
                "document_quantity": 0,
                "received_quantity": int(row["quantity"]),
            }
        )
    for item in receipt_items:
        errors = _marking_errors(item)
        if errors:
            lines.append(
                {
                    "kind": "marking",
                    "label": _label(item.oem_number, item.autopart_name),
                    "document_quantity": len(item.marking_codes or []),
                    "received_quantity": int(item.received_quantity or 0),
                    "marking_errors": errors,
                }
            )
    return lines


def _read_upd_items(document: DiadocIncomingDocument) -> Optional[list[UpdXmlItem]]:
    path = document.local_file_path
    if not path or not os.path.exists(os.path.abspath(path)):
        return None
    try:
        with open(os.path.abspath(path), "rb") as fh:
            payload = fh.read()
        if not looks_like_upd_xml(payload, os.path.basename(path)):
            return None
        return list(parse_upd_xml(payload).items or [])
    except Exception:
        return None


async def acceptance_summary(
    session: AsyncSession, document: DiadocIncomingDocument, *, lock: bool = False
) -> dict[str, Any]:
    """Итог сверки УПД с приёмкой и рекомендуемый код итога для подписи.

    ``checked=False`` — сверить нечем (нет XML или проведённых поступлений):
    требуется исправить документ перед подписанием титула покупателя.
    """
    summary: dict[str, Any] = {
        "checked": False,
        "has_discrepancies": False,
        "suggested_total_code": TOTAL_CODE_ACCEPTED,
        "lines": [],
        "lines_total": 0,
        "reason": None,
    }
    message = getattr(document, "supplier_order_message", None)
    receipts = list(getattr(message, "receipts", []) or [])
    if lock and document.supplier_order_message_id is not None:
        receipts = list((await session.scalars(
            select(SupplierReceipt)
            .where(SupplierReceipt.source_message_id == document.supplier_order_message_id)
            .order_by(SupplierReceipt.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )).all())
    receipt_ids = [int(receipt.id) for receipt in receipts]
    if not receipts or any(r.posted_at is None for r in receipts):
        summary["reason"] = "Поступление по документу ещё не проведено"
        return summary
    upd_items = _read_upd_items(document)
    if not upd_items or any(item.quantity is None for item in upd_items):
        summary["reason"] = "Не удалось прочитать товарные строки и количество из XML УПД"
        return summary
    receipt_items = (
        (
            await session.execute(
                select(SupplierReceiptItem).where(
                    SupplierReceiptItem.receipt_id.in_(receipt_ids)
                ).execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    lines = compare_upd_with_receipt(upd_items, receipt_items)
    scans = (await session.scalars(
        select(ReceiptMarkingScan).where(ReceiptMarkingScan.receipt_id.in_(receipt_ids))
        .execution_options(populate_existing=True)
    )).all()
    marked = await autoparts_marked_by_category(
        session, [item.autopart_id for item in receipt_items]
    )
    scans_by_line = defaultdict(set)
    parts = {item.id: item.autopart_id for item in receipt_items}
    for scan in scans:
        item_id = scan.receipt_item_id
        if scan.autopart_id is not None and parts.get(item_id) != scan.autopart_id:
            item_id = None
        scans_by_line[(scan.receipt_id, item_id)].add(scan.code)
    all_accepted = Counter(
        code for item in receipt_items for code in normalize_marking_codes(item.marking_codes)
    )
    for item in receipt_items:
        codes = set(normalize_marking_codes(item.marking_codes))
        scanned = scans_by_line.pop((item.receipt_id, item.id), set())
        if not (codes or scanned or item.autopart_id in marked):
            continue
        matched = codes & scanned
        qty = int(item.received_quantity or 0)
        if (
            matched != codes
            or scanned != codes
            or qty != len(matched)
            or any(all_accepted[c] > 1 for c in codes)
        ):
            lines.append(
                {
                    "kind": "marking",
                    "label": f"{_label(item.oem_number, item.autopart_name)}: сверка сканов",
                    "document_quantity": qty,
                    "received_quantity": len(matched),
                }
            )
    extra = sum(len(codes) for codes in scans_by_line.values())
    if extra:
        lines.append({"kind": "marking", "label": "Лишние сканы вне строк поступления",
                      "document_quantity": 0, "received_quantity": extra})
    summary["checked"] = True
    summary["lines_total"] = len(lines)
    summary["lines"] = lines[:_MAX_LINES]
    if lines:
        summary["has_discrepancies"] = True
        nothing_received = all(
            int(item.received_quantity or 0) == 0 for item in receipt_items
        )
        summary["suggested_total_code"] = (
            TOTAL_CODE_NOT_ACCEPTED if nothing_received else TOTAL_CODE_WITH_DISCREPANCIES
        )
    return summary


class ReceiptAcceptanceBlocked(ValueError):
    """Проведённая приёмка не позволяет подписать выбранный результат."""


def assert_acceptance_result(summary: dict[str, Any], total_code: str) -> None:
    if not summary["checked"]:
        raise ReceiptAcceptanceBlocked(summary["reason"] or "Не удалось сверить входящий УПД")
    if total_code == TOTAL_CODE_ACCEPTED and summary["has_discrepancies"]:
        raise ReceiptAcceptanceBlocked(
            "Приёмка расходится с УПД. Проверьте сканы, "
            "подпишите с расхождениями или оформите отказ."
        )
