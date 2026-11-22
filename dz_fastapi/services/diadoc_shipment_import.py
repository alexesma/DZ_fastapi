"""Реализации по исходящим УПД Диадока, оформленным вне системы.

Реализации оформляют в 1С, а УПД уходит клиенту через Диадок. Чтобы такие
продажи были видны в системе (список реализаций, анализ), по УПД создаётся
``ShipmentDocument`` со статусом ``IMPORTED``. Он не списывает остатки, не
уходит в 1С и не попадает в конвейер ЭДО: те отбирают только DRAFT/POSTED.

Защита от дублей: один УПД — одна реализация (``source_diadoc_document_id``),
а если в системе уже есть наша реализация того же клиента с той же суммой
в пределах нескольких дней, УПД считается её отправкой и не импортируется.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import MOSCOW_TZ, now_moscow
from dz_fastapi.models.autopart import AutoPart, preprocess_oem_number
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentStatus,
    SyncStatus,
)
from dz_fastapi.models.partner import Customer
from dz_fastapi.services.diadoc_direction import get_our_inn
from dz_fastapi.services.upd_xml import UpdXmlDocument, extract_document_parties, parse_upd_xml

logger = logging.getLogger("dz_fastapi")

UPD_TYPE = "UniversalTransferDocument"
DUPLICATE_WINDOW_DAYS = 3
KNOWN_VAT_RATES = (Decimal("0"), Decimal("10"), Decimal("20"), Decimal("22"))
DEFAULT_VAT = Decimal("22")


def _q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _vat_rate(net_price: Optional[float], total: Optional[float], qty: Optional[int]) -> Decimal:
    """Ставка НДС по строке: брутто / нетто, привязанная к известным ставкам."""
    if not net_price or not total or not qty:
        return DEFAULT_VAT
    ratio = Decimal(str(total)) / (Decimal(str(net_price)) * Decimal(qty))
    percent = (ratio - 1) * 100
    return min(KNOWN_VAT_RATES, key=lambda rate: abs(rate - percent))


async def _find_customer(
    session: AsyncSession, outgoing: DiadocOutgoingDocument, buyer_inn: Optional[str]
) -> Optional[Customer]:
    if outgoing.customer_id:
        return await session.get(Customer, outgoing.customer_id)
    if buyer_inn:
        return (
            (await session.execute(select(Customer).where(Customer.inn == buyer_inn)))
            .scalars()
            .first()
        )
    return None


async def _find_autopart_id(session: AsyncSession, article: Optional[str]) -> Optional[int]:
    if not article:
        return None
    normalized = preprocess_oem_number(str(article))
    if not normalized:
        return None
    return (
        await session.scalar(
            select(AutoPart.id).where(AutoPart.oem_number == normalized).order_by(AutoPart.id)
        )
    )


def _document_datetime(upd: UpdXmlDocument, outgoing: DiadocOutgoingDocument) -> datetime:
    day: Optional[date] = upd.shipment_date or upd.document_date or outgoing.document_date
    if day is None:
        return now_moscow()
    return datetime(day.year, day.month, day.day, 12, 0, tzinfo=MOSCOW_TZ)


async def _duplicate_of_system_shipment(
    session: AsyncSession,
    *,
    customer_id: Optional[int],
    total: Decimal,
    moment: datetime,
) -> Optional[int]:
    """Наша собственная реализация того же клиента на ту же сумму рядом по дате."""
    if customer_id is None or total <= 0:
        return None
    rows = (
        (
            await session.execute(
                select(ShipmentDocument).where(
                    ShipmentDocument.customer_id == customer_id,
                    ShipmentDocument.status.in_(
                        [ShipmentDocumentStatus.DRAFT, ShipmentDocumentStatus.POSTED]
                    ),
                    ShipmentDocument.doc_date >= moment - timedelta(days=DUPLICATE_WINDOW_DAYS),
                    ShipmentDocument.doc_date <= moment + timedelta(days=DUPLICATE_WINDOW_DAYS),
                )
            )
        )
        .scalars()
        .all()
    )
    for shipment in rows:
        shipment_total = sum(
            (Decimal(str(item.price or 0)) * item.quantity for item in shipment.items),
            Decimal("0"),
        )
        if abs(_q2(shipment_total) - total) <= Decimal("1.00"):
            return int(shipment.id)
    return None


def _mark(outgoing: DiadocOutgoingDocument, **info: Any) -> None:
    outgoing.metadata_json = {**(outgoing.metadata_json or {}), "shipment_import": info}


async def import_shipment_from_outgoing_upd(
    session: AsyncSession,
    outgoing: DiadocOutgoingDocument,
    *,
    our_inn: Optional[str] = None,
) -> dict[str, Any]:
    """Создаёт импортную реализацию по одному исходящему УПД."""
    existing = await session.scalar(
        select(ShipmentDocument.id).where(ShipmentDocument.source_diadoc_document_id == outgoing.id)
    )
    if existing is not None:
        return {"created": False, "reason": "already_imported", "shipment_id": existing}
    if outgoing.type_named_id != UPD_TYPE:
        return {"created": False, "reason": "not_upd"}
    if outgoing.status in ("revoked", "rejected"):
        _mark(outgoing, skipped="revoked")
        session.add(outgoing)
        return {"created": False, "reason": "revoked"}
    path = os.path.abspath(outgoing.local_file_path) if outgoing.local_file_path else None
    if not path or not os.path.exists(path):
        return {"created": False, "reason": "no_file"}

    with open(path, "rb") as fh:
        payload = fh.read()
    try:
        upd = parse_upd_xml(payload)
    except ValueError as exc:
        _mark(outgoing, skipped="unparsable", error=str(exc)[:200])
        session.add(outgoing)
        return {"created": False, "reason": "unparsable"}
    parties = extract_document_parties(payload)
    if our_inn and parties and parties.seller_inn and parties.seller_inn != our_inn:
        _mark(outgoing, skipped="seller_is_not_us")
        session.add(outgoing)
        return {"created": False, "reason": "seller_is_not_us"}
    if not upd.items:
        _mark(outgoing, skipped="no_items")
        session.add(outgoing)
        return {"created": False, "reason": "no_items"}

    customer = await _find_customer(session, outgoing, parties.buyer_inn if parties else None)
    moment = _document_datetime(upd, outgoing)
    total = _q2(Decimal(str(upd.total_with_vat or 0)))
    duplicate_id = await _duplicate_of_system_shipment(
        session,
        customer_id=customer.id if customer else None,
        total=total,
        moment=moment,
    )
    if duplicate_id is not None:
        _mark(outgoing, skipped="system_shipment_exists", shipment_id=duplicate_id)
        session.add(outgoing)
        return {"created": False, "reason": "system_shipment_exists", "shipment_id": duplicate_id}

    number = upd.document_number or outgoing.document_number
    document = ShipmentDocument(
        doc_number=str(number) if number else None,
        doc_date=moment,
        status=ShipmentDocumentStatus.IMPORTED,
        customer_id=customer.id if customer else None,
        reason=f"Импорт из Диадока: УПД № {number or '—'} от {moment:%d.%m.%Y}",
        sync_status=SyncStatus.SYNCED,
        source_diadoc_document_id=outgoing.id,
    )
    session.add(document)
    await session.flush()

    matched = 0
    unmatched: list[str] = []
    for line in upd.items:
        if not line.quantity or line.quantity <= 0:
            continue
        autopart_id = await _find_autopart_id(session, line.oem_number)
        if autopart_id is None:
            unmatched.append(str(line.oem_number or line.name or line.line_number))
            continue
        gross_unit = (
            _q2(Decimal(str(line.total_with_vat)) / line.quantity)
            if line.total_with_vat
            else _q2(Decimal(str(line.price or 0)))
        )
        session.add(
            ShipmentDocumentItem(
                document_id=document.id,
                autopart_id=autopart_id,
                customer_oem=line.oem_number,
                customer_name=(line.name or "")[:512] or None,
                quantity=line.quantity,
                price=gross_unit,
                vat_rate=_vat_rate(line.price, line.total_with_vat, line.quantity),
            )
        )
        matched += 1
    notes = [f"Сумма УПД с НДС: {total}. Строк в УПД: {len(upd.items)}, сопоставлено: {matched}."]
    if unmatched:
        notes.append("Не найдены в номенклатуре: " + ", ".join(unmatched[:30]))
    if customer is None:
        notes.append("Клиент не определён по ящику и ИНН покупателя.")
    document.notes = "\n".join(notes)
    _mark(outgoing, shipment_id=document.id, matched=matched, unmatched=len(unmatched))
    session.add_all([document, outgoing])
    await session.flush()
    return {
        "created": True,
        "reason": "created",
        "shipment_id": document.id,
        "matched": matched,
        "unmatched": len(unmatched),
    }


async def import_pending_upd_shipments(
    session: AsyncSession, *, environment: str, limit: int = 200
) -> dict[str, Any]:
    """Реализации по ещё не обработанным внешним исходящим УПД."""
    our_inn = await get_our_inn(session, environment)
    rows = (
        (
            await session.execute(
                select(DiadocOutgoingDocument)
                .outerjoin(
                    ShipmentDocument,
                    ShipmentDocument.source_diadoc_document_id == DiadocOutgoingDocument.id,
                )
                .where(
                    DiadocOutgoingDocument.environment == environment,
                    DiadocOutgoingDocument.source_type == "diadoc_external",
                    DiadocOutgoingDocument.type_named_id == UPD_TYPE,
                    ShipmentDocument.id.is_(None),
                    DiadocOutgoingDocument.metadata_json[("shipment_import", "skipped")]
                    .as_string()
                    .is_(None),
                )
                .order_by(DiadocOutgoingDocument.id)
                .limit(max(1, int(limit)))
            )
        )
        .scalars()
        .all()
    )
    result: dict[str, Any] = {"checked": 0, "created": 0, "skipped": {}, "errors": []}
    for outgoing in rows:
        result["checked"] += 1
        try:
            async with session.begin_nested():
                item = await import_shipment_from_outgoing_upd(session, outgoing, our_inn=our_inn)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Shipment import failed for diadoc doc %s", outgoing.id)
            result["errors"].append(f"{outgoing.id}: {exc}"[:300])
            continue
        if item["created"]:
            result["created"] += 1
        else:
            reason = item["reason"]
            result["skipped"][reason] = result["skipped"].get(reason, 0) + 1
    await session.commit()
    return result
