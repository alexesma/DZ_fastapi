"""Автоматическая привязка входящего УКД поставщика к возврату.

Привязка не меняет склад и хозяйственный статус возврата: товар уже списан
при отгрузке поставщику. Здесь фиксируется только юридический документооборот.
"""

from __future__ import annotations

import os
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.inventory import ReturnDocumentStatus, ReturnItem, ReturnToSupplier
from dz_fastapi.models.partner import (
    SupplierReceiptCorrection,
    SupplierReceiptCorrectionItem,
    SupplierReceiptItem,
)
from dz_fastapi.services.marking_code_parse import return_unit_codes
from dz_fastapi.services.upd_xml import CorrectionDocument, CorrectionRow, parse_correction_xml

_ALNUM_RE = re.compile(r"[^0-9A-Za-zА-Яа-яЁё]+")


def _oem_key(value: object) -> str:
    return _ALNUM_RE.sub("", str(value or "")).upper()


def supplier_return_diadoc_status(document: DiadocIncomingDocument) -> str:
    revocation = str(document.revocation_status or "").lower()
    recipient = str(document.recipient_response_status or "").lower()
    if revocation == "revocationaccepted":
        return "revoked"
    if document.rejected_at is not None or any(
        marker in recipient for marker in ("signaturerequestrejected", "rejected", "rejection")
    ):
        return "rejected"
    if document.signed_at is not None or document.docflow_is_finished is True:
        return "signed"
    return "received"


async def sync_supplier_return_ukd_status(
    session: AsyncSession,
    document: DiadocIncomingDocument,
) -> ReturnToSupplier | None:
    """Синхронизирует юридический статус без повторного движения товара."""
    linked = (
        await session.execute(
            select(ReturnToSupplier).where(
                ReturnToSupplier.diadoc_incoming_document_id == document.id
            )
        )
    ).scalar_one_or_none()
    if linked is None:
        return None
    status = supplier_return_diadoc_status(document)
    if linked.diadoc_status != status:
        linked.diadoc_status = status
        linked.diadoc_status_updated_at = now_moscow()
        session.add(linked)
        await session.flush()
    return linked


def _read_correction(document: DiadocIncomingDocument) -> CorrectionDocument | None:
    path = os.path.abspath(str(document.local_file_path or ""))
    if not path or not os.path.isfile(path):
        return None
    with open(path, "rb") as source:
        return parse_correction_xml(source.read())


def _row_for_receipt_item(
    parsed: CorrectionDocument,
    receipt_item: SupplierReceiptItem,
) -> CorrectionRow | None:
    matches = [
        row
        for row in parsed.rows
        if row.article and _oem_key(row.article) == _oem_key(receipt_item.oem_number)
    ]
    return matches[0] if len(matches) == 1 else None


def _marking_matches(
    return_item: ReturnItem,
    receipt_item: SupplierReceiptItem,
    parsed: CorrectionDocument,
) -> tuple[bool, str | None]:
    row = _row_for_receipt_item(parsed, receipt_item)
    return_codes = list(return_item.marking_codes or [])
    if row is None:
        if return_codes:
            return False, "Для маркированной строки не найдена однозначная строка УКД"
        return True, None
    before_raw = list(row.marking_codes_before or [])
    after_raw = list(row.marking_codes_after or [])
    if not return_codes and not before_raw and not after_raw:
        return True, None
    if not return_codes:
        return False, "В УКД есть КИ, но в возврате поставщику они не сохранены"
    try:
        before = return_unit_codes(before_raw, row.quantity_before)
        after = return_unit_codes(after_raw, row.quantity_after)
        returned = return_unit_codes(return_codes, return_item.quantity)
    except ValueError as exc:
        return False, str(exc)
    if not set(after).issubset(before):
        return False, "Состав КИ после корректировки не является остатком строки до неё"
    removed = [code for code in before if code not in set(after)]
    if set(removed) != set(returned) or len(removed) != len(returned):
        return False, "Возвращаемые КИ не совпадают с разницей КИ в УКД"
    return True, None


async def _candidate_matches(
    session: AsyncSession,
    candidate: ReturnToSupplier,
    *,
    correction_quantities: dict[int, int],
    receipt_items: dict[int, SupplierReceiptItem],
    parsed: CorrectionDocument,
) -> tuple[bool, str | None]:
    return_quantities: dict[int, int] = {}
    for item in candidate.items or []:
        if item.supplier_receipt_item_id is None:
            return False, "В возврате есть строка без связи со строкой поступления"
        receipt_item_id = int(item.supplier_receipt_item_id)
        if receipt_item_id in return_quantities:
            return False, "Строка поступления повторяется в возврате"
        return_quantities[receipt_item_id] = int(item.quantity or 0)
        receipt_item = receipt_items.get(receipt_item_id)
        if receipt_item is None:
            return False, "Не найдена исходная строка поступления"
        marking_ok, reason = _marking_matches(item, receipt_item, parsed)
        if not marking_ok:
            return False, reason
    if return_quantities != correction_quantities:
        return False, "Состав или количество строк возврата не совпадает с УКД"
    return True, None


async def match_supplier_return_ukd(
    session: AsyncSession,
    *,
    document: DiadocIncomingDocument,
    correction: SupplierReceiptCorrection,
) -> dict[str, Any]:
    """Однозначно привязывает входящий УКД к отгруженному возврату."""
    already_linked = (
        await session.execute(
            select(ReturnToSupplier).where(
                ReturnToSupplier.diadoc_incoming_document_id == document.id
            )
        )
    ).scalar_one_or_none()
    if already_linked is not None:
        await sync_supplier_return_ukd_status(session, document)
        return {"state": "linked", "return_to_supplier_id": int(already_linked.id)}

    if correction.receipt_id is None:
        return {
            "state": "receipt_not_linked",
            "hint": "Сначала сопоставьте УКД с исходным поступлением",
        }
    parsed = _read_correction(document)
    if parsed is None:
        return {"state": "not_parsed", "hint": "Не удалось прочитать XML УКД"}

    correction_rows = (
        (
            await session.execute(
                select(SupplierReceiptCorrectionItem)
                .where(SupplierReceiptCorrectionItem.correction_id == correction.id)
                .order_by(SupplierReceiptCorrectionItem.id)
            )
        )
        .scalars()
        .all()
    )
    correction_item_ids = {int(row.receipt_item_id) for row in correction_rows}
    receipt_items = {
        int(row.id): row
        for row in (
            (
                await session.execute(
                    select(SupplierReceiptItem).where(
                        SupplierReceiptItem.id.in_(correction_item_ids)
                    )
                )
            )
            .scalars()
            .all()
        )
    }
    correction_quantities: dict[int, int] = {}
    for correction_row in correction_rows:
        receipt_item_id = int(correction_row.receipt_item_id)
        receipt_item = receipt_items.get(receipt_item_id)
        xml_row = (
            _row_for_receipt_item(parsed, receipt_item)
            if receipt_item is not None
            else None
        )
        if xml_row is None:
            return {
                "state": "unsupported_correction",
                "hint": "Строки УКД нельзя однозначно связать со строками поступления",
            }
        delta = int(xml_row.quantity_before or 0) - int(xml_row.quantity_after or 0)
        if delta <= 0 or receipt_item_id in correction_quantities:
            return {
                "state": "unsupported_correction",
                "hint": "Автопривязка поддерживает только однозначное уменьшение количества",
            }
        correction_quantities[receipt_item_id] = delta
    if not correction_quantities:
        return {
            "state": "no_quantity_reduction",
            "hint": "В УКД нет уменьшения количества",
        }
    candidates = (
        (
            await session.execute(
                select(ReturnToSupplier)
                .options(selectinload(ReturnToSupplier.items))
                .where(
                    ReturnToSupplier.provider_id == correction.provider_id,
                    ReturnToSupplier.supplier_receipt_id == correction.receipt_id,
                    ReturnToSupplier.status.in_(
                        [ReturnDocumentStatus.SHIPPED, ReturnDocumentStatus.CONFIRMED]
                    ),
                    ReturnToSupplier.diadoc_incoming_document_id.is_(None),
                )
                .order_by(ReturnToSupplier.id)
            )
        )
        .scalars()
        .unique()
        .all()
    )
    matches: list[ReturnToSupplier] = []
    rejected: list[dict[str, Any]] = []
    for candidate in candidates:
        matched, reason = await _candidate_matches(
            session,
            candidate,
            correction_quantities=correction_quantities,
            receipt_items=receipt_items,
            parsed=parsed,
        )
        if matched:
            matches.append(candidate)
        else:
            rejected.append({"id": int(candidate.id), "reason": reason})

    if len(matches) != 1:
        state = "return_not_found" if not matches else "return_ambiguous"
        return {
            "state": state,
            "candidate_ids": [int(row.id) for row in matches],
            "rejected_candidates": rejected[:10],
            "hint": (
                "Подходящий отгруженный возврат поставщику не найден"
                if not matches
                else "Найдено несколько одинаковых возвратов — выберите нужный вручную"
            ),
        }

    linked = matches[0]
    linked.diadoc_incoming_document_id = document.id
    linked.supplier_receipt_correction_id = correction.id
    linked.diadoc_status = supplier_return_diadoc_status(document)
    linked.diadoc_status_updated_at = now_moscow()
    session.add(linked)
    await session.flush()
    return {
        "state": "linked",
        "return_to_supplier_id": int(linked.id),
        "diadoc_status": linked.diadoc_status,
    }
