"""Conservative automatic EnterpriseData dispatch and transport tracking.

The UNF ``ReceivedNo`` proves only that a message was read.  Posting is a
separate accounting result and must never be inferred from that number.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from decimal import Decimal
from typing import Any
from xml.etree import ElementTree as ET

from sqlalchemy import and_, case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.inventory import (
    LotSourceType,
    ShipmentDocument,
    ShipmentDocumentStatus,
    StockDocument,
    StockDocumentItem,
    StockLot,
    StockMovement,
)
from dz_fastapi.models.one_c import OneCEnterpriseConfig, OneCExchangeEvent
from dz_fastapi.models.partner import (
    Provider,
    SupplierOrderItem,
    SupplierReceipt,
    SupplierReceiptItem,
)
from dz_fastapi.models.settings import CompanyProfile
from dz_fastapi.services.one_c_enterprise_data import get_exchange_status, send_body_message
from dz_fastapi.services.one_c_enterprise_data_export import DATA_NS, event_body_objects
from dz_fastapi.services.one_c_outbox import (
    ENTITY_RECEIPT,
    ENTITY_SHIPMENT,
    EVENT_POSTED,
    build_receipt_snapshot,
    build_shipment_snapshot,
    enqueue_one_c_event,
)

logger = logging.getLogger("dz_fastapi")
_EVENT_ID_RE = re.compile(r"\bevent(?:\s+|:)(\d+)\s*$")
_EVENT_IDS_RE = re.compile(r"\bevents:([0-9,]+)\s*$")
_DISPATCH_LOCK_KEY = 148_271_002
_MAX_BATCH_SIZE = 50


def config_data(config: OneCEnterpriseConfig | None) -> dict[str, Any]:
    return {
        "enabled": bool(config.enabled) if config else False,
        "send_receipts": bool(config.send_receipts) if config else True,
        "send_shipments": bool(config.send_shipments) if config else False,
        "batch_size": config.batch_size if config else 1,
        "first_event_id": config.first_event_id if config else None,
    }


async def set_config(
    session: AsyncSession,
    *,
    enabled: bool,
    send_receipts: bool,
    send_shipments: bool,
    include_history: bool = False,
    batch_size: int | None = None,
) -> OneCEnterpriseConfig:
    """A new activation starts at the current queue tail unless history is chosen."""
    if batch_size is not None and not 1 <= batch_size <= _MAX_BATCH_SIZE:
        raise ValueError("Размер партии EnterpriseData должен быть от 1 до 50")
    config = await session.get(OneCEnterpriseConfig, 1, with_for_update=True)
    if config is None:
        config = OneCEnterpriseConfig(id=1)
        session.add(config)
    if enabled and not config.enabled and config.first_event_id is None:
        if include_history:
            config.first_event_id = 1
        else:
            last_id = await session.scalar(select(func.max(OneCExchangeEvent.id))) or 0
            config.first_event_id = last_id + 1
    config.enabled = enabled
    config.send_receipts = send_receipts
    config.send_shipments = send_shipments
    if batch_size is not None:
        config.batch_size = batch_size
    config.updated_at = now_moscow()
    await session.flush()
    return config


def _event_ids(body: dict[str, Any] | None) -> list[int]:
    if not body:
        return []
    description = str(body.get("description") or "")
    batch = _EVENT_IDS_RE.search(description)
    if batch:
        return [int(value) for value in batch.group(1).split(",") if value]
    match = _EVENT_ID_RE.search(description)
    return [int(match.group(1))] if match else []


async def reconcile_transport(
    session: AsyncSession, transport: dict[str, Any],
) -> list[int]:
    """Recover DB tracking even after a crash between FTP write and DB commit."""
    changed: list[int] = []
    for field_name, confirmed in (("pending_body", False), ("last_confirmed_body", True)):
        body = transport.get(field_name)
        event_ids = _event_ids(body)
        if not event_ids:
            continue
        message_no = int(body["message_no"])
        for event_id in event_ids:
            event = await session.get(OneCExchangeEvent, event_id)
            if event is None:
                logger.error("EnterpriseData message refers to missing event %s", event_id)
                continue
            if event.ed_message_no is not None and event.ed_message_no != message_no:
                logger.error(
                    "EnterpriseData event %s has message %s, FTP says %s",
                    event_id, event.ed_message_no, message_no,
                )
                continue
            event.ed_message_no = message_no
            if event.ed_sent_at is None and body.get("sent_at"):
                event.ed_sent_at = datetime.fromisoformat(body["sent_at"])
            if confirmed and event.ed_transport_confirmed_at is None:
                event.ed_transport_confirmed_at = datetime.fromisoformat(
                    body.get("transport_confirmed_at") or now_moscow().isoformat()
                )
            event.ed_last_error = None
            if event_id not in changed:
                changed.append(event_id)
    await session.flush()
    return changed


async def _company(session: AsyncSession) -> dict[str, Any]:
    profile = (await session.execute(select(CompanyProfile))).scalars().first()
    if profile is None:
        raise ValueError("Не заполнены реквизиты организации")
    return {
        "short_name": profile.short_name,
        "full_name": profile.full_name,
        "inn": profile.inn,
        "kpp": profile.kpp,
        "is_individual_entrepreneur": profile.is_individual_entrepreneur,
    }


async def _current_payload(session: AsyncSession, event: OneCExchangeEvent) -> dict:
    if event.entity_type == ENTITY_RECEIPT:
        receipt = await session.get(SupplierReceipt, event.entity_id)
        if receipt is None or receipt.posted_at is None:
            raise ValueError("Поступление отменено или удалено")
        if (
            receipt.supplier_order_id
            and (receipt.comment or "").startswith("Авто-черновик поступления из ответа поставщика")
        ):
            ordered_qty = await session.scalar(
                select(func.sum(SupplierOrderItem.quantity)).where(
                    SupplierOrderItem.supplier_order_id == receipt.supplier_order_id
                )
            )
            received_qty = sum(
                Decimal(str(item.get("quantity") or 0))
                for item in event.payload.get("items") or []
            )
            if ordered_qty and received_qty > 10 * ordered_qty:
                raise ValueError(
                    f"Поступление №{receipt.id}: {received_qty} шт. против "
                    f"{ordered_qty} шт. в заказе поставщику №{receipt.supplier_order_id}; "
                    "проверьте авто-черновик и исходное письмо"
                )
        party = event.payload.get("provider") or {}
        provider = await session.get(Provider, party.get("id")) if party.get("id") else None
        if provider is None:
            raise ValueError("Поставщик поступления не найден")
        if not provider.one_c_contract_guid and not (
            provider.one_c_contract_name and provider.one_c_contract_number
            and provider.one_c_contract_date
        ):
            raise ValueError("Для поставщика не задан договор УНФ")
        return {
            **event.payload,
            "provider": {
                **party,
                "one_c_contract": {
                    "name": provider.one_c_contract_name,
                    "number": provider.one_c_contract_number,
                    "date": provider.one_c_contract_date,
                    "guid": provider.one_c_contract_guid,
                },
            },
        }
    if event.entity_type == ENTITY_SHIPMENT:
        shipment = await session.get(ShipmentDocument, event.entity_id)
        if shipment is None or shipment.status != ShipmentDocumentStatus.POSTED:
            raise ValueError("Реализация отменена или удалена")
        return event.payload
    raise ValueError("Тип документа не поддерживается EnterpriseData")


async def dispatch_next(session: AsyncSession) -> dict[str, Any]:
    """Send one bounded message, after reconciling any prior FTP confirmation."""
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": _DISPATCH_LOCK_KEY}
        )
    transport = await asyncio.to_thread(get_exchange_status)
    reconciled = await reconcile_transport(session, transport)
    config = await session.get(OneCEnterpriseConfig, 1)
    result: dict[str, Any] = {
        "reconciled": reconciled, "sent_event_id": None, "sent_event_ids": [],
    }
    if config is None or not config.enabled:
        result["note"] = "Автоотправка выключена"
        return result
    if transport["incoming_files"]:
        result["note"] = "Сначала нужно обработать входящее сообщение УНФ"
        return result
    if transport["pending_body"]:
        result["note"] = "Ожидаем чтения предыдущего сообщения УНФ"
        return result
    entity_types = []
    if config.send_receipts:
        entity_types.append(ENTITY_RECEIPT)
    if config.send_shipments:
        entity_types.append(ENTITY_SHIPMENT)
    if not entity_types:
        result["note"] = "Не выбран ни один тип документа"
        return result

    events = (
        await session.execute(
            select(OneCExchangeEvent)
            .where(
                OneCExchangeEvent.id >= (config.first_event_id or 1),
                OneCExchangeEvent.entity_type.in_(entity_types),
                OneCExchangeEvent.event_type == EVENT_POSTED,
                OneCExchangeEvent.ed_message_no.is_(None),
                OneCExchangeEvent.ed_last_error.is_(None),
            )
            .order_by(
                OneCExchangeEvent.payload["document_date"].as_string(),
                case((OneCExchangeEvent.entity_type == ENTITY_RECEIPT, 0), else_=1),
                OneCExchangeEvent.id,
            )
            .limit(min(max(config.batch_size or 1, 1), _MAX_BATCH_SIZE))
        )
    ).scalars().all()
    if not events:
        result["note"] = "Документов для отправки нет"
        return result
    try:
        company = await _company(session)
    except ValueError as exc:
        result["note"] = str(exc)
        return result
    references: list[ET.Element] = []
    documents: list[ET.Element] = []
    seen_references: set[tuple[str, str | bytes]] = set()
    for event in events:
        try:
            payload = await _current_payload(session, event)
            body_objects = event_body_objects(
                event.entity_type, event.event_type, payload, company
            )
        except (KeyError, TypeError, ValueError) as exc:
            event.ed_last_error = str(exc)
            await session.flush()
            result.update({"error_event_id": event.id, "note": str(exc)})
            return result
        for obj in body_objects:
            if obj.tag.rsplit("}", 1)[-1].startswith("Справочник."):
                ref = obj.find(f"{{{DATA_NS}}}КлючевыеСвойства/{{{DATA_NS}}}Ссылка")
                identity = ref.text if ref is not None and ref.text else ET.tostring(obj)
                marker = (obj.tag, identity)
                if marker not in seen_references:
                    seen_references.add(marker)
                    references.append(obj)
            else:
                documents.append(obj)
    event_ids = [event.id for event in events]
    description = (
        f"{events[0].entity_type}:{events[0].entity_id} event:{event_ids[0]}"
        if len(events) == 1 else "batch events:" + ",".join(map(str, event_ids))
    )
    try:
        sent = await asyncio.to_thread(
            send_body_message,
            references + documents,
            description=description,
        )
    except (KeyError, TypeError, ValueError) as exc:
        result["note"] = str(exc)
        return result
    sent_at = now_moscow()
    for event in events:
        event.ed_message_no = sent["message_no"]
        event.ed_sent_at = sent_at
        event.ed_last_error = None
    await session.flush()
    result.update({
        "sent_event_id": event_ids[0], "sent_event_ids": event_ids,
        "message_no": sent["message_no"],
    })
    return result


async def history_audit(session: AsyncSession) -> dict[str, Any]:
    """Read-only inventory of the historical transfer before activation."""
    receipt_events = and_(
        OneCExchangeEvent.entity_type == ENTITY_RECEIPT,
        OneCExchangeEvent.entity_id == SupplierReceipt.id,
        OneCExchangeEvent.event_type == EVENT_POSTED,
    )
    shipment_events = and_(
        OneCExchangeEvent.entity_type == ENTITY_SHIPMENT,
        OneCExchangeEvent.entity_id == ShipmentDocument.id,
        OneCExchangeEvent.event_type == EVENT_POSTED,
    )
    receipts = (
        await session.execute(
            select(
                func.count(SupplierReceipt.id),
                func.min(SupplierReceipt.document_date),
                func.max(SupplierReceipt.document_date),
            ).where(SupplierReceipt.posted_at.is_not(None))
        )
    ).one()
    shipments = (
        await session.execute(
            select(
                func.count(ShipmentDocument.id),
                func.min(ShipmentDocument.doc_date),
                func.max(ShipmentDocument.doc_date),
            ).where(ShipmentDocument.status == ShipmentDocumentStatus.POSTED)
        )
    ).one()
    receipt_missing = await session.scalar(
        select(func.count(SupplierReceipt.id))
        .outerjoin(OneCExchangeEvent, receipt_events)
        .where(SupplierReceipt.posted_at.is_not(None), OneCExchangeEvent.id.is_(None))
    )
    shipment_missing = await session.scalar(
        select(func.count(ShipmentDocument.id))
        .outerjoin(OneCExchangeEvent, shipment_events)
        .where(
            ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
            OneCExchangeEvent.id.is_(None),
        )
    )
    missing_contracts = (
        await session.execute(
            select(
                Provider.id,
                Provider.name,
                func.count(SupplierReceipt.id),
            )
            .join(SupplierReceipt, SupplierReceipt.provider_id == Provider.id)
            .where(
                SupplierReceipt.posted_at.is_not(None),
                (
                    Provider.one_c_contract_guid.is_(None)
                    | (Provider.one_c_contract_guid == "")
                ),
                (
                    Provider.one_c_contract_name.is_(None)
                    | Provider.one_c_contract_number.is_(None)
                    | Provider.one_c_contract_date.is_(None)
                ),
            )
            .group_by(Provider.id, Provider.name)
            .order_by(func.count(SupplierReceipt.id).desc())
            .limit(100)
        )
    ).all()
    lots = (
        await session.execute(
            select(
                StockLot.source_type,
                func.count(StockLot.id),
                func.coalesce(func.sum(StockLot.initial_quantity), 0),
                func.coalesce(func.sum(StockLot.remaining_quantity), 0),
            ).group_by(StockLot.source_type)
        )
    ).all()
    movements = (
        await session.execute(
            select(StockMovement.movement_type, func.count(StockMovement.id))
            .group_by(StockMovement.movement_type)
        )
    ).all()
    other_lots = (
        await session.execute(
            select(
                StockLot.id,
                StockLot.autopart_id,
                StockLot.source_type,
                StockLot.initial_quantity,
                StockLot.remaining_quantity,
                StockLot.cost_price,
                StockLot.received_at,
                StockDocument.id,
                StockDocument.status,
            )
            .outerjoin(
                StockDocumentItem,
                StockDocumentItem.id == StockLot.source_document_item_id,
            )
            .outerjoin(StockDocument, StockDocument.id == StockDocumentItem.document_id)
            .where(StockLot.source_type != LotSourceType.RECEIPT)
            .order_by(StockLot.received_at, StockLot.id)
            .limit(100)
        )
    ).all()
    ordered_quantities = (
        select(
            SupplierOrderItem.supplier_order_id.label("order_id"),
            func.sum(SupplierOrderItem.quantity).label("quantity"),
        )
        .group_by(SupplierOrderItem.supplier_order_id)
        .subquery()
    )
    receipt_quantities = (
        select(
            SupplierReceiptItem.receipt_id.label("receipt_id"),
            func.sum(SupplierReceiptItem.received_quantity).label("quantity"),
        )
        .group_by(SupplierReceiptItem.receipt_id)
        .subquery()
    )
    suspicious_auto_receipts = (
        await session.execute(
            select(
                SupplierReceipt.id,
                Provider.name,
                SupplierReceipt.supplier_order_id,
                receipt_quantities.c.quantity,
                ordered_quantities.c.quantity,
            )
            .join(Provider, Provider.id == SupplierReceipt.provider_id)
            .join(receipt_quantities, receipt_quantities.c.receipt_id == SupplierReceipt.id)
            .join(
                ordered_quantities,
                ordered_quantities.c.order_id == SupplierReceipt.supplier_order_id,
            )
            .where(
                SupplierReceipt.posted_at.is_not(None),
                SupplierReceipt.comment.like("Авто-черновик поступления из ответа поставщика%"),
                ordered_quantities.c.quantity > 0,
                receipt_quantities.c.quantity > 10 * ordered_quantities.c.quantity,
            )
            .order_by(SupplierReceipt.id)
            .limit(100)
        )
    ).all()
    missing_price_receipts = (
        await session.execute(
            select(SupplierReceipt.id, Provider.name, func.count(SupplierReceiptItem.id))
            .join(Provider, Provider.id == SupplierReceipt.provider_id)
            .join(SupplierReceiptItem, SupplierReceiptItem.receipt_id == SupplierReceipt.id)
            .where(
                SupplierReceipt.posted_at.is_not(None),
                SupplierReceiptItem.received_quantity > 0,
                (
                    SupplierReceiptItem.price.is_(None)
                    | (SupplierReceiptItem.price <= 0)
                ),
            )
            .group_by(SupplierReceipt.id, Provider.name)
            .order_by(SupplierReceipt.id)
            .limit(100)
        )
    ).all()
    return {
        "posted_receipts": {
            "count": receipts[0], "first_date": receipts[1], "last_date": receipts[2],
            "without_event": receipt_missing,
            "without_date": await session.scalar(
                select(func.count(SupplierReceipt.id)).where(
                    SupplierReceipt.posted_at.is_not(None),
                    SupplierReceipt.document_date.is_(None),
                )
            ),
        },
        "posted_shipments": {
            "count": shipments[0], "first_date": shipments[1], "last_date": shipments[2],
            "without_event": shipment_missing,
        },
        "providers_missing_contract": [
            {"id": row[0], "name": row[1], "posted_receipts": row[2]}
            for row in missing_contracts
        ],
        "suspicious_auto_receipts": [
            {
                "receipt_id": row[0], "provider": row[1],
                "supplier_order_id": row[2], "received_quantity": row[3],
                "ordered_quantity": row[4],
            }
            for row in suspicious_auto_receipts
        ],
        "missing_price_receipts": [
            {"receipt_id": row[0], "provider": row[1], "items": row[2]}
            for row in missing_price_receipts
        ],
        "stock_lots_by_source": [
            {
                "source": str(row[0]), "lots": row[1],
                "initial_quantity": row[2], "remaining_quantity": row[3],
            }
            for row in lots
        ],
        "non_receipt_lots": [
            {
                "id": row[0], "autopart_id": row[1], "source": str(row[2]),
                "initial_quantity": row[3], "remaining_quantity": row[4],
                "cost_price": row[5], "received_at": row[6],
                "stock_document_id": row[7],
                "stock_document_status": str(row[8]) if row[8] is not None else None,
            }
            for row in other_lots
        ],
        "movements_by_type": [
            {"type": str(row[0]), "count": row[1]} for row in movements
        ],
    }


async def backfill_history_step(session: AsyncSession, *, limit: int = 100) -> dict:
    """Create EnterpriseData-only snapshots; never feed historical rows to CommerceML."""
    if not 1 <= limit <= 100:
        raise ValueError("За один проход можно подготовить от 1 до 100 документов")
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": _DISPATCH_LOCK_KEY}
        )
    config = await session.get(OneCEnterpriseConfig, 1)
    if config is not None and config.enabled:
        raise ValueError("На время подготовки истории выключите автоотправку")
    created: list[int] = []
    for model, entity_type, date_field, builder in (
        (SupplierReceipt, ENTITY_RECEIPT, SupplierReceipt.document_date, build_receipt_snapshot),
        (ShipmentDocument, ENTITY_SHIPMENT, ShipmentDocument.doc_date, build_shipment_snapshot),
    ):
        if len(created) >= limit:
            break
        event_join = and_(
            OneCExchangeEvent.entity_type == entity_type,
            OneCExchangeEvent.entity_id == model.id,
            OneCExchangeEvent.event_type == EVENT_POSTED,
        )
        posted_filter = (
            model.posted_at.is_not(None) if entity_type == ENTITY_RECEIPT
            else model.status == ShipmentDocumentStatus.POSTED
        )
        ids = (
            await session.execute(
                select(model.id)
                .outerjoin(OneCExchangeEvent, event_join)
                .where(posted_filter, OneCExchangeEvent.id.is_(None))
                .order_by(date_field, model.id)
                .limit(limit - len(created))
            )
        ).scalars().all()
        for document_id in ids:
            payload = await builder(session, document_id)
            event = await enqueue_one_c_event(
                session,
                entity_type=entity_type,
                entity_id=document_id,
                event_type=EVENT_POSTED,
                payload=payload,
                ed_only=True,
            )
            created.append(event.id)
    return {"created_event_ids": created, "count": len(created)}
