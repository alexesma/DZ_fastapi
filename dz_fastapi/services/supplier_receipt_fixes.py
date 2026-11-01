"""Исправление даты поступления по дате документа из XML-вложения."""

from __future__ import annotations

import os
from datetime import date
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import SupplierOrderMessage, SupplierReceipt
from dz_fastapi.services.upd_xml import looks_like_upd_xml, parse_upd_xml


def document_date_from_xml(payload: bytes, filename: str) -> Optional[date]:
    """Дата отгрузки/передачи, иначе дата СФ — как при разборе поступления."""
    if not looks_like_upd_xml(payload, filename):
        return None
    try:
        parsed = parse_upd_xml(payload)
    except ValueError:
        return None
    return parsed.shipment_date or parsed.document_date


async def fix_receipt_document_date(
    session: AsyncSession, *, receipt_id: int, apply: bool = False
) -> dict[str, Any]:
    """Сверяет дату поступления с датой УПД из исходного сообщения.

    apply=False — только отчёт. apply=True — меняет только document_date
    (остатки, проведение и строки не затрагиваются) и дописывает запись
    в комментарий поступления: история изменений у поступления в системе
    ведётся там.
    """
    receipt = (
        await session.execute(
            select(SupplierReceipt)
            .options(
                selectinload(SupplierReceipt.source_message).selectinload(
                    SupplierOrderMessage.attachments
                )
            )
            .where(SupplierReceipt.id == receipt_id)
        )
    ).scalar_one_or_none()
    if receipt is None:
        return {"receipt_id": receipt_id, "status": "not_found"}
    message = receipt.source_message
    xml_date = None
    for attachment in (message.attachments if message else []) or []:
        path = str(attachment.file_path or "")
        if not path.lower().endswith(".xml") or not os.path.exists(
            os.path.abspath(path)
        ):
            continue
        with open(os.path.abspath(path), "rb") as fh:
            xml_date = document_date_from_xml(fh.read(), attachment.filename)
        if xml_date:
            break
    result = {
        "receipt_id": receipt_id,
        "current_date": receipt.document_date,
        "xml_date": xml_date,
        "posted": receipt.posted_at is not None,
    }
    if xml_date is None:
        return {**result, "status": "no_xml_date"}
    if receipt.document_date == xml_date:
        return {**result, "status": "ok"}
    if not apply:
        return {**result, "status": "needs_fix"}
    note = (
        f"[{now_moscow():%Y-%m-%d %H:%M}] Дата документа исправлена "
        f"с {receipt.document_date} на {xml_date} (дата УПД из вложения)"
    )
    receipt.document_date = xml_date
    receipt.comment = f"{receipt.comment}\n{note}" if receipt.comment else note
    session.add(receipt)
    await session.commit()
    return {**result, "status": "fixed"}
