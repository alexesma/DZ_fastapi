"""Акт о расхождениях при приёмке (DP_PRIRASXPRIN…) → черновик рекламации.

Покупатель (наш клиент) сообщает, что при приёмке нашей реализации
обнаружены расхождения/брак. Мы находим реализацию по номеру и дате
сопроводительного УПД, создаём черновик рекламации по строкам с браком и
ничего не проводим: возврат оформляет человек.
"""

from __future__ import annotations

import os
import re
from datetime import date, timedelta
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.inventory import ShipmentDocument, ShipmentDocumentItem
from dz_fastapi.models.partner import (
    RECLAMATION_ITEM_SOURCE,
    RECLAMATION_SOURCE,
    RECLAMATION_STATUS,
    RECLAMATION_TYPE,
    Reclamation,
    ReclamationEvent,
    ReclamationItem,
)
from dz_fastapi.services.diadoc_outgoing import resolve_customer_by_diadoc_counteragent_box
from dz_fastapi.services.supplier_document_dedup import normalize_document_number
from dz_fastapi.services.upd_xml import parse_discrepancy_act_xml

DATE_TOLERANCE_DAYS = 3
_ALNUM_RE = re.compile(r"[^0-9A-Za-zА-Яа-яЁё]+")
RECLAMATION_KEY_PREFIX = "diadoc-act:"
OPEN_RECLAMATION_STATUSES = {
    RECLAMATION_STATUS.NEW,
    RECLAMATION_STATUS.RECOGNIZED,
    RECLAMATION_STATUS.CHECKED,
    RECLAMATION_STATUS.WAITING_DOCS,
    RECLAMATION_STATUS.WAITING_SUPPLIER,
}


def _key(value: object) -> str:
    return _ALNUM_RE.sub("", str(value or "")).upper()


def _as_date(value: object) -> Optional[date]:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


async def find_shipment(
    session: AsyncSession,
    *,
    number: Optional[str],
    doc_date: Optional[date],
    customer_id: Optional[int],
) -> tuple[Optional[ShipmentDocument], list[ShipmentDocument]]:
    """Реализация по номеру (серийная часть) и дате ±3 дня; кандидаты."""
    wanted = normalize_document_number(number)
    stmt = (
        select(ShipmentDocument)
        .options(selectinload(ShipmentDocument.items))
        .where(ShipmentDocument.doc_number.is_not(None))
        .order_by(ShipmentDocument.id.desc())
    )
    if customer_id:
        stmt = stmt.where(ShipmentDocument.customer_id == customer_id)
    if doc_date:
        stmt = stmt.where(
            ShipmentDocument.doc_date.between(
                doc_date - timedelta(days=DATE_TOLERANCE_DAYS * 10),
                doc_date + timedelta(days=DATE_TOLERANCE_DAYS * 10),
            )
        )
    rows = list((await session.execute(stmt.limit(300))).scalars().all())
    same_number = [
        r for r in rows if wanted and normalize_document_number(r.doc_number) == wanted
    ]
    exact = [
        r
        for r in same_number
        if not doc_date
        or abs((_doc_date(r) - doc_date).days) <= DATE_TOLERANCE_DAYS
    ]
    if len(exact) == 1:
        return exact[0], same_number
    return None, same_number or rows[:5]


def _doc_date(shipment: ShipmentDocument) -> date:
    value = shipment.doc_date
    return value.date() if hasattr(value, "date") else value


async def _match_shipment_item(
    session: AsyncSession, shipment: ShipmentDocument, article: Optional[str]
) -> tuple[Optional[ShipmentDocumentItem], Optional[AutoPart]]:
    wanted = _key(article)
    if not wanted:
        return None, None
    ids = [i.autopart_id for i in shipment.items if i.autopart_id]
    if not ids:
        return None, None
    parts = {
        p.id: p
        for p in (
            await session.execute(select(AutoPart).where(AutoPart.id.in_(ids)))
        )
        .scalars()
        .all()
    }
    for item in shipment.items:
        part = parts.get(item.autopart_id)
        if part is not None and _key(part.oem_number) == wanted:
            return item, part
    return None, None


async def process_discrepancy_act(
    session: AsyncSession, *, document: DiadocIncomingDocument
) -> dict[str, Any]:
    """Разбирает акт, ищет реализацию и создаёт черновик рекламации."""
    path = document.local_file_path
    payload = None
    if path and os.path.exists(os.path.abspath(path)):
        with open(os.path.abspath(path), "rb") as fh:
            payload = fh.read()
    details = parse_discrepancy_act_xml(payload) if payload else None
    if not details:
        return {"state": "not_parsed", "hint": "XML акта не разобран"}
    defects = [r for r in details["rows"] if (r.get("qty_defect") or 0) > 0]
    details["defect_rows"] = len(defects)
    customer = await resolve_customer_by_diadoc_counteragent_box(
        session, document.counteragent_box_id
    )
    shipment, candidates = await find_shipment(
        session,
        number=details.get("basis_number"),
        doc_date=_as_date(details.get("basis_date")),
        customer_id=int(customer.id) if customer else None,
    )
    details["customer_id"] = int(customer.id) if customer else None
    if shipment is None:
        details["state"] = "shipment_not_found"
        details["candidates"] = [
            {"id": int(c.id), "number": c.doc_number,
             "date": str(c.doc_date)[:10] if c.doc_date else None}
            for c in candidates[:10]
        ]
        details["hint"] = (
            "Реализация не найдена — привяжите вручную "
            f"(УПД №{details.get('basis_number') or '—'} "
            f"от {details.get('basis_date') or '—'})"
        )
        return details
    details["shipment_id"] = int(shipment.id)
    if not defects:
        details["state"] = "no_defects"
        details["hint"] = "В акте нет строк с браком: рекламация не создана"
        return details

    key = (
        f"{RECLAMATION_KEY_PREFIX}{details.get('buyer_inn')}:"
        f"{details.get('number')}:{details.get('date')}"
    )
    existing = (
        await session.execute(
            select(Reclamation).where(Reclamation.email_message_id == key)
        )
    ).scalars().first()
    if existing is not None:
        details.update(state="reclamation_exists", reclamation_id=int(existing.id))
        details["hint"] = "Рекламация по этому акту уже создана"
        return details

    reclamation = Reclamation(
        source=RECLAMATION_SOURCE.MANUAL,
        status=RECLAMATION_STATUS.RECOGNIZED,
        reclamation_type=RECLAMATION_TYPE.DEFECT,
        customer_id=shipment.customer_id or (int(customer.id) if customer else None),
        email_message_id=key,
        email_subject=(
            f"Акт о расхождениях №{details.get('number')} от {details.get('date')} (Диадок)"
        ),
        stated_document_number=str(details.get("basis_number") or "") or None,
        stated_document_date=_as_date(details.get("basis_date")),
        stated_reason=details.get("conclusion") or details.get("event"),
        extracted_data={
            "diadoc_document_id": int(document.id),
            "shipment_id": int(shipment.id),
            "act": {k: v for k, v in details.items() if k not in {"result", "basis", "rows"}},
            "rows": details["rows"],
        },
    )
    session.add(reclamation)
    await session.flush()
    for row in defects:
        item, part = await _match_shipment_item(session, shipment, row.get("article"))
        session.add(
            ReclamationItem(
                reclamation_id=reclamation.id,
                oem_number=row.get("article"),
                autopart_name=row.get("name"),
                quantity=int(row["qty_defect"]),
                reason=(
                    f"Акт о расхождениях: брак {row['qty_defect']} шт., "
                    f"сумма {row.get('sum_defect')} ₽, решение покупателя: "
                    f"{row.get('decision') or '—'}"
                ),
                item_source=RECLAMATION_ITEM_SOURCE.UNKNOWN,
                autopart_id=part.id if part else None,
                shipment_item_id=item.id if item else None,
            )
        )
    session.add(
        ReclamationEvent(
            reclamation_id=reclamation.id,
            event_type="created_from_diadoc_act",
            details={"diadoc_document_id": int(document.id), "act_number": details.get("number")},
        )
    )
    await session.flush()
    details.update(
        state="reclamation_created",
        reclamation_id=int(reclamation.id),
        hint="Создан черновик рекламации: проверьте и примите решение",
    )
    return details
