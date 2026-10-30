"""Сохранение номеров и статусов УНФ в событиях обмена."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.one_c import OneCExchangeEvent
from dz_fastapi.services.one_c_enterprise_data import read_inbox
from dz_fastapi.services.one_c_enterprise_data_export import (
    event_document_number,
    event_document_ref,
)

logger = logging.getLogger("dz_fastapi")

_ENTITY_BY_OBJECT = {
    "Документ.РеализацияТоваровУслуг": "shipment",
    "Документ.ПоступлениеТоваровУслуг": "supplier_receipt",
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
    events: list[OneCExchangeEvent], entity_type: str,
) -> tuple[dict[str, OneCExchangeEvent], dict[str, OneCExchangeEvent]]:
    by_ref: dict[str, OneCExchangeEvent] = {}
    by_number: dict[str, OneCExchangeEvent] = {}
    for event in events:  # от новых к старым: у документа побеждает последнее состояние
        document_id = (event.payload or {}).get("document_id")
        if document_id is None:
            continue
        try:
            by_ref.setdefault(event_document_ref(entity_type, document_id), event)
            by_number.setdefault(event_document_number(document_id), event)
        except ValueError:
            continue
    return by_ref, by_number


async def apply_inbox(
    session: AsyncSession, directory: Optional[str] = None,
) -> dict[str, Any]:
    """Переносит принятые от УНФ номера и статусы в события и очищает очередь.

    Вызывающий делает commit и затем ``drop_inbox(outcome["messages"])``:
    при сбое между ними повтор запишет те же значения.
    """
    inbox = await asyncio.to_thread(read_inbox, directory)
    outcome: dict[str, Any] = {"messages": len(inbox), "matched": 0, "unmatched": []}
    if not inbox:
        return outcome

    maps: dict[str, tuple[dict, dict]] = {}
    for entity_type in set(_ENTITY_BY_OBJECT.values()):
        maps[entity_type] = _key_maps(await _sent_events(session, entity_type), entity_type)

    received = now_moscow()
    for entry in inbox:
        for document in entry.get("documents") or []:
            entity_type = _ENTITY_BY_OBJECT.get(document.get("object_type"))
            if entity_type is None:
                continue
            by_ref, by_number = maps[entity_type]
            event = by_ref.get(document.get("ref") or "") or by_number.get(
                document.get("number") or ""
            )
            if event is None:
                outcome["unmatched"].append({
                    "message_no": entry.get("message_no"),
                    "object_type": document.get("object_type"),
                    "ref": document.get("ref"),
                    "number": document.get("number"),
                })
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
            outcome["matched"] += 1
    await session.flush()
    return outcome
