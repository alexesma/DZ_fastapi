"""Transactional outbox for reliable, idempotent exchange with 1C."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Optional

from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.autopart import AutoPart
from dz_fastapi.models.inventory import (
    ProductionWave,
    ProductionWaveAllocation,
    ProductionWaveItem,
    ShipmentDocument,
    ShipmentDocumentItem,
    ShipmentDocumentItemLotAllocation,
    ShipmentDocumentStatus,
    StockDocument,
    StockDocumentItem,
    SyncStatus,
)
from dz_fastapi.models.one_c import (
    OneCExchangeBatch,
    OneCExchangeBatchItem,
    OneCExchangeBatchStatus,
    OneCExchangeEvent,
    OneCExchangeEventStatus,
)
from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem

ENTITY_SHIPMENT = "shipment"
ENTITY_RECEIPT = "supplier_receipt"
ENTITY_STOCK_DOCUMENT = "stock_document"
ENTITY_PRODUCTION_WAVE = "production_wave"

EVENT_POSTED = "posted"
EVENT_CANCELLED = "cancelled"
EVENT_COMPLETED = "completed"

CHANNEL_COMMERCEML = "commerceml"
CHANNEL_JSON = "json"
OUTBOX_LOCK_KEY = 148_271_001


def _value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _item_autopart_payload(autopart: Optional[AutoPart]) -> dict[str, Any]:
    brand = getattr(getattr(autopart, "brand", None), "name", None)
    return {
        "autopart_id": getattr(autopart, "id", None),
        "oem": getattr(autopart, "oem_number", None),
        "brand": brand,
        "name": getattr(autopart, "name", None),
        "barcode": getattr(autopart, "barcode", None),
    }


async def build_shipment_snapshot(
    session: AsyncSession,
    shipment_id: int,
) -> dict[str, Any]:
    document = (
        await session.execute(
            select(ShipmentDocument)
            .where(ShipmentDocument.id == shipment_id)
            .options(
                selectinload(ShipmentDocument.items)
                .selectinload(ShipmentDocumentItem.autopart)
                .selectinload(AutoPart.brand),
                selectinload(ShipmentDocument.items)
                .selectinload(ShipmentDocumentItem.allocations)
                .selectinload(ShipmentDocumentItemLotAllocation.stock_lot),
                selectinload(ShipmentDocument.customer),
                selectinload(ShipmentDocument.warehouse),
            )
        )
    ).scalar_one_or_none()
    if document is None:
        raise LookupError("Реализация не найдена")
    customer = document.customer
    return {
        "document_id": document.id,
        "document_number": document.doc_number or f"DZ-{document.id}",
        "document_date": _value(document.doc_date),
        "status": _value(document.status),
        "customer_order_id": document.customer_order_id,
        "customer": {
            "id": getattr(customer, "id", None),
            "name": getattr(customer, "name", None),
            "inn": getattr(customer, "inn", None),
            "kpp": getattr(customer, "kpp", None),
            "legal_address": getattr(customer, "legal_address", None),
            "postal_address": getattr(customer, "postal_address", None),
            "email_contact": getattr(customer, "email_contact", None),
            "email_outgoing_price": getattr(
                customer, "email_outgoing_price", None
            ),
            "description": getattr(customer, "description", None),
            "comment": getattr(customer, "comment", None),
            "type_prices": _value(getattr(customer, "type_prices", None)),
            "credit_limit": _value(getattr(customer, "credit_limit", None)),
            "payment_terms_days": getattr(customer, "payment_terms_days", None),
        },
        "warehouse": {
            "id": getattr(document.warehouse, "id", None),
            "name": getattr(document.warehouse, "name", None),
        },
        "notes": document.notes,
        "items": [
            {
                "item_id": item.id,
                "physical": _item_autopart_payload(item.autopart),
                "customer_oem": item.customer_oem,
                "customer_brand": item.customer_brand,
                "customer_name": item.customer_name,
                "quantity": int(item.quantity or 0),
                "price": _value(item.price),
                "vat_rate": _value(item.vat_rate),
                "cost_price": _value(item.cost_price),
                "cost_total": _value(item.cost_total),
                "allocations": [
                    {
                        "stock_lot_id": allocation.stock_lot_id,
                        "provider_id": allocation.provider_id,
                        "quantity": int(allocation.quantity or 0),
                        "unit_cost_price": _value(allocation.unit_cost_price),
                        "total_cost_price": _value(allocation.total_cost_price),
                        "gtd_number": getattr(allocation.stock_lot, "gtd_number", None),
                        "country_code": getattr(allocation.stock_lot, "country_code", None),
                        "country_name": getattr(allocation.stock_lot, "country_name", None),
                        "marking_codes": list(allocation.marking_codes or []),
                    }
                    for allocation in (item.allocations or [])
                ],
            }
            for item in (document.items or [])
        ],
    }


async def build_receipt_snapshot(
    session: AsyncSession,
    receipt_id: int,
) -> dict[str, Any]:
    receipt = (
        await session.execute(
            select(SupplierReceipt)
            .where(SupplierReceipt.id == receipt_id)
            .options(
                selectinload(SupplierReceipt.provider),
                selectinload(SupplierReceipt.warehouse),
                selectinload(SupplierReceipt.items)
                .selectinload(SupplierReceiptItem.autopart)
                .selectinload(AutoPart.brand),
            )
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise LookupError("Поступление не найдено")
    return {
        "document_id": receipt.id,
        "document_number": receipt.document_number,
        "document_date": _value(receipt.document_date),
        "posted_at": _value(receipt.posted_at),
        "provider": {
            "id": getattr(receipt.provider, "id", None),
            "name": getattr(receipt.provider, "name", None),
            "inn": getattr(receipt.provider, "inn", None),
            "kpp": getattr(receipt.provider, "kpp", None),
        },
        "warehouse": {
            "id": getattr(receipt.warehouse, "id", None),
            "name": getattr(receipt.warehouse, "name", None),
        },
        "items": [
            {
                "item_id": item.id,
                "autopart": _item_autopart_payload(item.autopart),
                "oem": item.oem_number,
                "brand": item.brand_name,
                "name": item.autopart_name,
                "quantity": int(item.received_quantity or 0),
                "price": _value(item.price),
                "total_price_with_vat": _value(item.total_price_with_vat),
                "gtd_number": item.gtd_code,
                "country_code": item.country_code,
                "country_name": item.country_name,
                "marking_codes": list(item.marking_codes or []),
                "customer_order_item_id": item.customer_order_item_id,
            }
            for item in (receipt.items or [])
        ],
    }


async def build_stock_document_snapshot(
    session: AsyncSession,
    document_id: int,
) -> dict[str, Any]:
    document = (
        await session.execute(
            select(StockDocument)
            .where(StockDocument.id == document_id)
            .options(
                selectinload(StockDocument.items)
                .selectinload(StockDocumentItem.autopart)
                .selectinload(AutoPart.brand),
                selectinload(StockDocument.warehouse),
            )
        )
    ).scalar_one_or_none()
    if document is None:
        raise LookupError("Складской документ не найден")
    return {
        "document_id": document.id,
        "document_number": document.document_number,
        "document_date": _value(document.document_date),
        "document_type": _value(document.doc_type),
        "status": _value(document.status),
        "reason": document.reason,
        "warehouse": {
            "id": getattr(document.warehouse, "id", None),
            "name": getattr(document.warehouse, "name", None),
        },
        "items": [
            {
                "item_id": item.id,
                "autopart": _item_autopart_payload(item.autopart),
                "quantity": int(item.quantity or 0),
                "cost_price": _value(item.cost_price),
                "gtd_number": item.gtd_number,
                "country_code": item.country_code,
                "country_name": item.country_name,
            }
            for item in (document.items or [])
        ],
    }


async def build_production_wave_snapshot(
    session: AsyncSession,
    wave_id: int,
) -> dict[str, Any]:
    wave = (
        await session.execute(
            select(ProductionWave)
            .where(ProductionWave.id == wave_id)
            .options(
                selectinload(ProductionWave.items)
                .selectinload(ProductionWaveItem.finished_autopart)
                .selectinload(AutoPart.brand),
                selectinload(ProductionWave.items)
                .selectinload(ProductionWaveItem.allocations)
                .selectinload(ProductionWaveAllocation.material_autopart)
                .selectinload(AutoPart.brand),
                selectinload(ProductionWave.warehouse),
            )
        )
    ).scalar_one_or_none()
    if wave is None:
        raise LookupError("Производственная волна не найдена")
    return {
        "document_id": wave.id,
        "number": wave.number or f"DZ-WAVE-{wave.id}",
        "status": _value(wave.status),
        "completed_at": _value(wave.completed_at),
        "warehouse": {
            "id": getattr(wave.warehouse, "id", None),
            "name": getattr(wave.warehouse, "name", None),
        },
        "total_produced_quantity": int(wave.total_produced_quantity or 0),
        "total_material_cost": _value(wave.total_material_cost),
        "total_packaging_cost": _value(wave.total_packaging_cost),
        "total_finished_cost": _value(wave.total_finished_cost),
        "items": [
            {
                "item_id": item.id,
                "finished": _item_autopart_payload(item.finished_autopart),
                "quantity": int(item.produced_quantity or 0),
                "unit_cost": _value(item.unit_cost),
                "total_cost": _value(item.total_cost),
                "materials": [
                    {
                        "allocation_id": allocation.id,
                        "material": _item_autopart_payload(allocation.material_autopart),
                        "stock_lot_id": allocation.stock_lot_id,
                        "output_stock_lot_id": allocation.output_stock_lot_id,
                        "quantity": int(allocation.consumed_quantity or 0),
                        "gtd_number": allocation.gtd_number,
                        "country_code": allocation.country_code,
                        "country_name": allocation.country_name,
                        "marking_codes": list(allocation.marking_codes or []),
                    }
                    for allocation in (item.allocations or [])
                ],
            }
            for item in (wave.items or [])
        ],
    }


async def enqueue_one_c_event(
    session: AsyncSession,
    *,
    entity_type: str,
    entity_id: int,
    event_type: str,
    payload: dict[str, Any],
) -> OneCExchangeEvent:
    # A business transition is immutable. Its snapshot may nevertheless gain
    # derived values after first enqueue (for example FIFO allocations after
    # posting). Such changes must not create a second delivery to 1C.
    existing = (
        await session.execute(
            select(OneCExchangeEvent)
            .where(
                OneCExchangeEvent.entity_type == entity_type,
                OneCExchangeEvent.entity_id == entity_id,
                OneCExchangeEvent.event_type == event_type,
            )
            .order_by(OneCExchangeEvent.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return existing

    idempotency_key = f"{entity_type}:{entity_id}:{event_type}"
    event = OneCExchangeEvent(
        entity_type=entity_type,
        entity_id=entity_id,
        event_type=event_type,
        payload=payload,
        payload_version=1,
        idempotency_key=idempotency_key,
        status=OneCExchangeEventStatus.PENDING,
    )
    try:
        async with session.begin_nested():
            session.add(event)
            await session.flush()
    except IntegrityError:
        # A concurrent request may insert the same stable key after the lookup
        # above. The savepoint keeps the caller transaction usable.
        existing = (
            await session.execute(
                select(OneCExchangeEvent).where(
                    OneCExchangeEvent.idempotency_key == idempotency_key
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            return existing
        raise
    return event


async def enqueue_shipment_event(
    session: AsyncSession, shipment_id: int, event_type: str = EVENT_POSTED
) -> OneCExchangeEvent:
    payload = await build_shipment_snapshot(session, shipment_id)
    return await enqueue_one_c_event(
        session,
        entity_type=ENTITY_SHIPMENT,
        entity_id=shipment_id,
        event_type=event_type,
        payload=payload,
    )


async def enqueue_receipt_event(
    session: AsyncSession, receipt_id: int, event_type: str = EVENT_POSTED
) -> OneCExchangeEvent:
    payload = await build_receipt_snapshot(session, receipt_id)
    return await enqueue_one_c_event(
        session,
        entity_type=ENTITY_RECEIPT,
        entity_id=receipt_id,
        event_type=event_type,
        payload=payload,
    )


async def enqueue_stock_document_event(
    session: AsyncSession, document_id: int, event_type: str = EVENT_POSTED
) -> OneCExchangeEvent:
    payload = await build_stock_document_snapshot(session, document_id)
    return await enqueue_one_c_event(
        session,
        entity_type=ENTITY_STOCK_DOCUMENT,
        entity_id=document_id,
        event_type=event_type,
        payload=payload,
    )


async def enqueue_production_wave_event(
    session: AsyncSession, wave_id: int, event_type: str = EVENT_COMPLETED
) -> OneCExchangeEvent:
    payload = await build_production_wave_snapshot(session, wave_id)
    return await enqueue_one_c_event(
        session,
        entity_type=ENTITY_PRODUCTION_WAVE,
        entity_id=wave_id,
        event_type=event_type,
        payload=payload,
    )


async def backfill_pending_shipment_events(session: AsyncSession) -> int:
    ids = (
        (
            await session.execute(
                select(ShipmentDocument.id).where(
                    ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
                    ShipmentDocument.sync_status == SyncStatus.PENDING,
                )
            )
        )
        .scalars()
        .all()
    )
    before = int(
        (
            await session.execute(
                select(func.count(OneCExchangeEvent.id)).where(
                    OneCExchangeEvent.entity_type == ENTITY_SHIPMENT
                )
            )
        ).scalar_one()
    )
    for shipment_id in ids:
        event = await enqueue_shipment_event(session, int(shipment_id))
        if event.status in (
            OneCExchangeEventStatus.SUCCEEDED,
            OneCExchangeEventStatus.ERROR,
        ):
            await retry_one_c_event(session, event.id)
    after = int(
        (
            await session.execute(
                select(func.count(OneCExchangeEvent.id)).where(
                    OneCExchangeEvent.entity_type == ENTITY_SHIPMENT
                )
            )
        ).scalar_one()
    )
    return max(after - before, 0)


async def get_or_create_delivery_batch(
    session: AsyncSession,
    *,
    channel: str,
    entity_types: Iterable[str],
    event_types: Optional[Iterable[str]] = None,
    limit: int = 100,
) -> Optional[OneCExchangeBatch]:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:key)"),
        {"key": OUTBOX_LOCK_KEY},
    )
    active = (
        await session.execute(
            select(OneCExchangeBatch)
            .where(
                OneCExchangeBatch.channel == channel,
                OneCExchangeBatch.status == OneCExchangeBatchStatus.SENT,
            )
            .options(
                selectinload(OneCExchangeBatch.items).selectinload(OneCExchangeBatchItem.event)
            )
            .order_by(OneCExchangeBatch.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    now = now_moscow()
    if active is not None:
        active.attempt_count += 1
        active.last_sent_at = now
        for item in active.items:
            item.event.attempt_count += 1
            item.event.last_attempt_at = now
        await session.flush()
        return active

    event_stmt = (
        select(OneCExchangeEvent)
        .where(
            OneCExchangeEvent.status == OneCExchangeEventStatus.PENDING,
            OneCExchangeEvent.entity_type.in_(list(entity_types)),
            or_(
                OneCExchangeEvent.next_attempt_at.is_(None),
                OneCExchangeEvent.next_attempt_at <= now,
            ),
        )
        .order_by(OneCExchangeEvent.created_at, OneCExchangeEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    if event_types is not None:
        event_stmt = event_stmt.where(OneCExchangeEvent.event_type.in_(list(event_types)))
    events = list((await session.execute(event_stmt)).scalars().all())
    if not events:
        return None
    content_hash = hashlib.sha256(
        "|".join(event.idempotency_key for event in events).encode("utf-8")
    ).hexdigest()
    batch = OneCExchangeBatch(
        channel=channel,
        status=OneCExchangeBatchStatus.SENT,
        content_hash=content_hash,
        sent_at=now,
        last_sent_at=now,
        attempt_count=1,
    )
    session.add(batch)
    await session.flush()
    for event in events:
        event.status = OneCExchangeEventStatus.IN_FLIGHT
        event.attempt_count += 1
        event.last_attempt_at = now
        session.add(OneCExchangeBatchItem(batch_id=batch.id, event_id=event.id))
    await session.flush()
    return (
        await session.execute(
            select(OneCExchangeBatch)
            .where(OneCExchangeBatch.id == batch.id)
            .options(
                selectinload(OneCExchangeBatch.items).selectinload(OneCExchangeBatchItem.event)
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one()


async def _set_source_sync_state(
    session: AsyncSession,
    event: OneCExchangeEvent,
    *,
    status: SyncStatus,
    external_id: Optional[str] = None,
) -> None:
    model = {
        ENTITY_SHIPMENT: ShipmentDocument,
        ENTITY_STOCK_DOCUMENT: StockDocument,
        ENTITY_PRODUCTION_WAVE: ProductionWave,
    }.get(event.entity_type)
    if model is None:
        return
    source = await session.get(model, event.entity_id)
    if source is None:
        return
    source.sync_status = status
    if external_id:
        source.external_id = external_id


async def acknowledge_delivery_batch(
    session: AsyncSession,
    *,
    batch_uid: str,
    success: bool,
    error: Optional[str] = None,
    external_ids: Optional[dict[str, str]] = None,
    event_results: Optional[dict[str, dict[str, Any]]] = None,
) -> OneCExchangeBatch:
    batch = (
        await session.execute(
            select(OneCExchangeBatch)
            .where(OneCExchangeBatch.batch_uid == batch_uid)
            .options(
                selectinload(OneCExchangeBatch.items).selectinload(OneCExchangeBatchItem.event)
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if batch is None:
        raise LookupError("Пакет обмена с 1С не найден")
    if batch.status == OneCExchangeBatchStatus.SUCCEEDED:
        return batch
    now = now_moscow()
    external_ids = external_ids or {}
    event_results = event_results or {}
    if event_results:
        batch_event_uids = {item.event.event_uid for item in batch.items}
        result_event_uids = set(event_results)
        unknown = result_event_uids - batch_event_uids
        missing = batch_event_uids - result_event_uids
        if unknown:
            raise ValueError("Ответ 1С содержит неизвестные события: " + ", ".join(sorted(unknown)))
        if missing:
            raise ValueError(
                "В ответе 1С нет результатов для событий: " + ", ".join(sorted(missing))
            )

        failed_messages: list[str] = []
        for item in batch.items:
            event = item.event
            result = event_results[event.event_uid]
            event_success = bool(result.get("success"))
            external_id = str(result.get("external_id") or "").strip() or None
            event_error = str(result.get("error") or "").strip() or None
            event.confirmed_at = now
            if event_success:
                event.status = OneCExchangeEventStatus.SUCCEEDED
                event.external_id = external_id
                event.last_error = None
                await _set_source_sync_state(
                    session,
                    event,
                    status=SyncStatus.SYNCED,
                    external_id=external_id,
                )
            else:
                message = event_error or "1С отклонила документ"
                event.status = OneCExchangeEventStatus.ERROR
                event.last_error = message
                failed_messages.append(f"{event.event_uid}: {message}")
                await _set_source_sync_state(session, event, status=SyncStatus.ERROR)

        batch.confirmed_at = now
        if failed_messages:
            batch.status = OneCExchangeBatchStatus.ERROR
            batch.last_error = "; ".join(failed_messages)[:4000]
        else:
            batch.status = OneCExchangeBatchStatus.SUCCEEDED
            batch.last_error = None
    elif success:
        batch.status = OneCExchangeBatchStatus.SUCCEEDED
        batch.confirmed_at = now
        batch.last_error = None
        for item in batch.items:
            event = item.event
            external_id = external_ids.get(event.event_uid)
            event.status = OneCExchangeEventStatus.SUCCEEDED
            event.confirmed_at = now
            event.last_error = None
            if external_id:
                event.external_id = external_id
            await _set_source_sync_state(
                session,
                event,
                status=SyncStatus.SYNCED,
                external_id=external_id,
            )
    else:
        message = str(error or "1С отклонила пакет").strip()
        batch.status = OneCExchangeBatchStatus.ERROR
        batch.last_error = message
        for item in batch.items:
            event = item.event
            event.status = OneCExchangeEventStatus.ERROR
            event.last_error = message
            await _set_source_sync_state(session, event, status=SyncStatus.ERROR)
    await session.flush()
    return batch


async def acknowledge_latest_batch(
    session: AsyncSession,
    *,
    channel: str,
) -> Optional[OneCExchangeBatch]:
    batch_uid = (
        await session.execute(
            select(OneCExchangeBatch.batch_uid)
            .where(
                OneCExchangeBatch.channel == channel,
                OneCExchangeBatch.status == OneCExchangeBatchStatus.SENT,
            )
            .order_by(OneCExchangeBatch.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if batch_uid is None:
        return None
    return await acknowledge_delivery_batch(session, batch_uid=batch_uid, success=True)


async def retry_one_c_event(
    session: AsyncSession,
    event_id: int,
) -> OneCExchangeEvent:
    event = await session.get(OneCExchangeEvent, event_id)
    if event is None:
        raise LookupError("Событие обмена с 1С не найдено")
    active_batches = (
        (
            await session.execute(
                select(OneCExchangeBatch)
                .join(OneCExchangeBatchItem)
                .where(
                    OneCExchangeBatchItem.event_id == event.id,
                    OneCExchangeBatch.status == OneCExchangeBatchStatus.SENT,
                )
                .options(
                    selectinload(OneCExchangeBatch.items).selectinload(OneCExchangeBatchItem.event)
                )
            )
        )
        .scalars()
        .all()
    )
    for batch in active_batches:
        batch.status = OneCExchangeBatchStatus.ERROR
        batch.last_error = "Пакет отменён администратором для повторной отправки"
        for batch_item in batch.items:
            batch_event = batch_item.event
            batch_event.status = OneCExchangeEventStatus.PENDING
            batch_event.next_attempt_at = None
            batch_event.last_error = None
            await _set_source_sync_state(session, batch_event, status=SyncStatus.PENDING)
    event.status = OneCExchangeEventStatus.PENDING
    event.next_attempt_at = None
    event.last_error = None
    event.confirmed_at = None
    await _set_source_sync_state(session, event, status=SyncStatus.PENDING)
    await session.flush()
    return event


async def get_one_c_outbox_status(session: AsyncSession) -> dict[str, Any]:
    counts = dict(
        (
            await session.execute(
                select(OneCExchangeEvent.status, func.count()).group_by(OneCExchangeEvent.status)
            )
        ).all()
    )
    active_batch = (
        await session.execute(
            select(OneCExchangeBatch)
            .where(OneCExchangeBatch.status == OneCExchangeBatchStatus.SENT)
            .order_by(OneCExchangeBatch.id.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    return {
        "pending_events": int(counts.get(OneCExchangeEventStatus.PENDING, 0)),
        "in_flight_events": int(counts.get(OneCExchangeEventStatus.IN_FLIGHT, 0)),
        "succeeded_events": int(counts.get(OneCExchangeEventStatus.SUCCEEDED, 0)),
        "error_events": int(counts.get(OneCExchangeEventStatus.ERROR, 0)),
        "active_batch_uid": active_batch.batch_uid if active_batch else None,
        "active_batch_sent_at": active_batch.sent_at if active_batch else None,
    }


def serialize_one_c_event(
    event: OneCExchangeEvent,
    *,
    include_payload: bool = True,
) -> dict[str, Any]:
    result = {
        "id": event.id,
        "event_uid": event.event_uid,
        "entity_type": event.entity_type,
        "entity_id": event.entity_id,
        "event_type": event.event_type,
        "payload_version": event.payload_version,
        "idempotency_key": event.idempotency_key,
        "status": _value(event.status),
        "attempt_count": event.attempt_count,
        "last_attempt_at": event.last_attempt_at,
        "confirmed_at": event.confirmed_at,
        "external_id": event.external_id,
        "last_error": event.last_error,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
    }
    if include_payload:
        result["payload"] = event.payload
    return result


def serialize_one_c_batch(batch: OneCExchangeBatch) -> dict[str, Any]:
    return {
        "id": batch.id,
        "batch_uid": batch.batch_uid,
        "channel": batch.channel,
        "status": _value(batch.status),
        "content_hash": batch.content_hash,
        "attempt_count": batch.attempt_count,
        "event_count": len(batch.items or []),
        "event_uids": [item.event.event_uid for item in (batch.items or [])],
        "sent_at": batch.sent_at,
        "last_sent_at": batch.last_sent_at,
        "confirmed_at": batch.confirmed_at,
        "last_error": batch.last_error,
        "created_at": batch.created_at,
    }
