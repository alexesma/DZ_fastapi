"""Сохранение номеров и статусов УНФ в событиях обмена."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import (
    ReturnFromCustomer,
    ReturnToSupplier,
    ShipmentDocument,
    StockDocument,
    SyncStatus,
)
from dz_fastapi.models.one_c import OneCExchangeEvent
from dz_fastapi.models.partner import SupplierReceipt
from dz_fastapi.services.one_c_enterprise_data import read_inbox
from dz_fastapi.services.one_c_enterprise_data_export import (
    event_document_number,
    event_document_ref,
)

logger = logging.getLogger("dz_fastapi")

_ENTITY_BY_OBJECT = {
    "Документ.РеализацияТоваровУслуг": "shipment",
    "Документ.ПоступлениеТоваровУслуг": "supplier_receipt",
    "Документ.ЗаказКлиента": "customer_order",
    "Документ.ЗаказПоставщику": "supplier_order",
    "Документ.ВозвратТоваровОтПокупателя": "customer_return",
    "Документ.ВозвратТоваровПоставщику": "supplier_return",
    "Документ.ОприходованиеТоваров": "stock_document",
    "Документ.СписаниеТоваров": "stock_document",
}


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=now_moscow().tzinfo)


async def _sent_events(session: AsyncSession, entity_type: str) -> list[OneCExchangeEvent]:
    rows = await session.execute(
        select(OneCExchangeEvent)
        .where(
            OneCExchangeEvent.entity_type == entity_type,
            OneCExchangeEvent.ed_message_no.is_not(None),
        )
        .order_by(OneCExchangeEvent.id.desc())
    )
    return list(rows.scalars())


def _key_maps(
    events: list[OneCExchangeEvent],
    entity_type: str,
) -> tuple[dict[str, OneCExchangeEvent], dict[str, OneCExchangeEvent]]:
    by_ref: dict[str, OneCExchangeEvent] = {}
    by_number: dict[str, OneCExchangeEvent] = {}
    for event in events:  # от новых к старым: у документа побеждает последнее состояние
        document_id = (event.payload or {}).get("document_id")
        if document_id is None:
            continue
        try:
            by_ref.setdefault(
                event_document_ref(entity_type, document_id, event.payload or {}),
                event,
            )
            by_number.setdefault(event_document_number(document_id), event)
        except ValueError:
            continue
    return by_ref, by_number


async def apply_inbox(
    session: AsyncSession,
    directory: Optional[str] = None,
) -> dict[str, Any]:
    """Переносит принятые от УНФ номера и статусы в события и очищает очередь.

    Вызывающий делает commit и затем ``drop_inbox(outcome["messages"])``:
    при сбое между ними повтор запишет те же значения.
    """
    inbox = await asyncio.to_thread(read_inbox, directory)
    outcome: dict[str, Any] = {
        "messages": len(inbox),
        "acknowledge_messages": 0,
        "matched": 0,
        "unmatched": [],
    }
    if not inbox:
        return outcome

    maps: dict[str, tuple[dict, dict]] = {}
    for entity_type in set(_ENTITY_BY_OBJECT.values()):
        maps[entity_type] = _key_maps(await _sent_events(session, entity_type), entity_type)

    received = now_moscow()
    can_ack_prefix = True
    for entry in inbox:
        entry_unmatched = 0
        for document in entry.get("documents") or []:
            entity_type = _ENTITY_BY_OBJECT.get(document.get("object_type"))
            if entity_type is None:
                continue
            by_ref, by_number = maps[entity_type]
            event = by_ref.get(document.get("ref") or "") or by_number.get(
                document.get("number") or ""
            )
            if event is None:
                entry_unmatched += 1
                outcome["unmatched"].append(
                    {
                        "message_no": entry.get("message_no"),
                        "object_type": document.get("object_type"),
                        "ref": document.get("ref"),
                        "number": document.get("number"),
                    }
                )
                logger.warning("UNF document without DragonZap event: %s", document)
                continue
            event.ed_unf_ref = document.get("ref")
            event.ed_unf_number = document.get("number")
            event.ed_unf_date = _parse_date(document.get("date"))
            if document.get("deleted"):
                event.ed_unf_posted = False
            elif document.get("posted") is not None:
                event.ed_unf_posted = document["posted"]
            event.ed_unf_received_at = received
            await _apply_source_result(session, event, document, received)
            outcome["matched"] += 1
        if can_ack_prefix and entry_unmatched == 0:
            outcome["acknowledge_messages"] += 1
        else:
            can_ack_prefix = False
    await session.flush()
    return outcome


async def _apply_source_result(
    session: AsyncSession,
    event: OneCExchangeEvent,
    document: dict[str, Any],
    received_at: datetime,
) -> None:
    """Отражает результат УНФ на исходном документе DragonZap.

    Для поступления номер поставщика менять нельзя. Для реализации учётный
    номер УНФ становится номером документа после первого успешного ответа.
    """
    deleted = bool(document.get("deleted"))
    posted = False if deleted else document.get("posted")
    expected_posted = event.event_type == "posted"
    synced = posted is expected_posted if posted is not None else False
    source_error: str | None = None

    if event.entity_type == "shipment":
        source = await session.get(ShipmentDocument, event.entity_id)
        if source is None:
            return
        source.external_id = document.get("ref") or source.external_id
        source.sync_status = SyncStatus.SYNCED if synced else SyncStatus.ERROR
        if expected_posted and posted is True and str(document.get("number") or "").strip():
            unf_number = str(document["number"]).strip()
            existing_diadoc = await session.scalar(
                select(DiadocOutgoingDocument.id).where(
                    DiadocOutgoingDocument.source_type == "shipment_document",
                    DiadocOutgoingDocument.source_id == source.id,
                    DiadocOutgoingDocument.document_number.is_not(None),
                    DiadocOutgoingDocument.document_number != unf_number,
                ).limit(1)
            )
            if existing_diadoc is not None:
                source.sync_status = SyncStatus.ERROR
                source_error = (
                    "УНФ вернула номер после формирования документа Диадока с другим номером. "
                    "Автоматическая замена запрещена; документ нужно сверить вручную."
                )
            else:
                source.doc_number = unf_number
        session.add(source)
    elif event.entity_type == "supplier_receipt":
        # SupplierReceipt.document_number — номер накладной поставщика. Он не
        # должен заменяться внутренним номером УНФ; результат хранится в событии.
        source = await session.get(SupplierReceipt, event.entity_id)
        if source is not None:
            session.add(source)
    elif event.entity_type in {"customer_return", "supplier_return", "stock_document"}:
        model = {
            "customer_return": ReturnFromCustomer,
            "supplier_return": ReturnToSupplier,
            "stock_document": StockDocument,
        }[event.entity_type]
        source = await session.get(model, event.entity_id)
        if source is not None:
            source.external_id = document.get("ref") or source.external_id
            source.sync_status = SyncStatus.SYNCED if synced else SyncStatus.ERROR
            unf_number = str(document.get("number") or "").strip()
            if expected_posted and posted is True and unf_number:
                diadoc_source_type = {
                    "customer_return": "return_from_customer",
                    "supplier_return": "return_to_supplier",
                    "stock_document": None,
                }[event.entity_type]
                existing_diadoc = None
                if diadoc_source_type:
                    existing_diadoc = await session.scalar(
                        select(DiadocOutgoingDocument.id)
                        .where(
                            DiadocOutgoingDocument.source_type == diadoc_source_type,
                            DiadocOutgoingDocument.source_id == source.id,
                            DiadocOutgoingDocument.document_number.is_not(None),
                            DiadocOutgoingDocument.document_number != unf_number,
                        )
                        .limit(1)
                    )
                if existing_diadoc is not None:
                    source.sync_status = SyncStatus.ERROR
                    source_error = (
                        "УНФ вернула номер после формирования документа Диадока с другим "
                        "номером. Автоматическая замена запрещена."
                    )
                elif event.entity_type == "stock_document":
                    source.document_number = unf_number
                else:
                    source.doc_number = unf_number
            session.add(source)

    if source_error:
        event.ed_last_error = source_error
    elif posted is not None and not synced:
        event.ed_last_error = (
            "УНФ вернула состояние, не соответствующее операции DragonZap: "
            f"ожидалось {'проведено' if expected_posted else 'не проведено'}, "
            f"получено {'проведено' if posted else 'не проведено'}"
        )
    else:
        event.ed_last_error = None
    event.ed_unf_received_at = received_at
