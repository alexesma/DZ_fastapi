"""Durable Diadoc BoxEvent feed with per-box cursor and retryable processing."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.diadoc import (
    DiadocBoxEvent,
    DiadocEventCursor,
    DiadocIncomingDocument,
    DiadocOutgoingDocument,
)
from dz_fastapi.services.diadoc_docflow import derive_docflow_state
from dz_fastapi.services.diadoc_documents import sync_diadoc_incoming_document_by_id

logger = logging.getLogger("dz_fastapi")


async def get_event_cursor(
    session: AsyncSession, *, environment: str, box_id_guid: str
) -> DiadocEventCursor | None:
    return (
        await session.execute(
            select(DiadocEventCursor).where(
                DiadocEventCursor.environment == environment,
                DiadocEventCursor.box_id_guid == box_id_guid,
                DiadocEventCursor.scope == "inbound",
            )
        )
    ).scalar_one_or_none()


async def initialize_event_cursor(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    boundary_event: dict[str, Any] | None = None,
) -> DiadocEventCursor:
    cursor = await stage_event_cursor_boundary(
        session,
        client=client,
        environment=environment,
        box_id_guid=box_id_guid,
        boundary_event=boundary_event,
    )
    if cursor is not None and cursor.initialized_at is not None:
        return cursor
    cursor.initialized_at = now_moscow()
    cursor.last_polled_at = cursor.initialized_at
    cursor.last_error = None
    await session.flush()
    return cursor


async def stage_event_cursor_boundary(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    boundary_event: dict[str, Any] | None = None,
) -> DiadocEventCursor:
    """Фиксирует границу до исторического обхода, но ещё не включает ленту.

    Если первичный обход занимает несколько запусков, граница остаётся той же:
    события, пришедшие за это время, будут прочитаны после завершения истории.
    """
    cursor = await get_event_cursor(session, environment=environment, box_id_guid=box_id_guid)
    if cursor is not None and cursor.index_key:
        return cursor
    boundary = boundary_event or await client.get_last_event(box_id_guid=box_id_guid)
    index_key = str(boundary.get("IndexKey") or "").strip() or None
    if cursor is None:
        cursor = DiadocEventCursor(
            environment=environment,
            box_id_guid=box_id_guid,
            scope="inbound",
        )
        session.add(cursor)
    cursor.index_key = index_key
    cursor.last_polled_at = now_moscow()
    cursor.last_error = None
    await session.flush()
    return cursor


def _event_message(event: dict[str, Any]) -> tuple[str, str | None, dict[str, Any]]:
    message = event.get("Message")
    patch = event.get("Patch")
    if isinstance(message, dict):
        return "message", str(message.get("MessageId") or "").strip() or None, message
    if isinstance(patch, dict):
        return "patch", str(patch.get("MessageId") or "").strip() or None, patch
    return "unknown", None, {}


async def fetch_event_page(
    session: AsyncSession,
    *,
    client: DiadocClient,
    cursor: DiadocEventCursor,
    limit: int = 100,
) -> dict[str, Any]:
    payload = await client.get_new_events(
        box_id_guid=cursor.box_id_guid,
        after_index_key=cursor.index_key,
        # Без фильтра по направлению: события исходящих (подпись, отказ,
        # аннулирование контрагентом) нужны для статусов наших отправок.
        limit=limit,
        order_by="Ascending",
    )
    events = list(payload.get("Events") or [])
    created = 0
    for raw in events:
        event_id = str(raw.get("EventId") or "").strip()
        index_key = str(raw.get("IndexKey") or "").strip()
        if not event_id or not index_key:
            raise ValueError("Диадок вернул событие без EventId/IndexKey")
        existing = (
            await session.execute(
                select(DiadocBoxEvent).where(
                    DiadocBoxEvent.environment == cursor.environment,
                    DiadocBoxEvent.box_id_guid == cursor.box_id_guid,
                    DiadocBoxEvent.event_id == event_id,
                )
            )
        ).scalar_one_or_none()
        kind, message_id, _ = _event_message(raw)
        if existing is None:
            session.add(
                DiadocBoxEvent(
                    environment=cursor.environment,
                    box_id_guid=cursor.box_id_guid,
                    event_id=event_id,
                    index_key=index_key,
                    event_kind=kind,
                    message_id=message_id,
                    payload=raw,
                )
            )
            created += 1
        cursor.index_key = index_key
    cursor.last_polled_at = now_moscow()
    cursor.last_error = None
    await session.flush()
    return {
        "received": len(events),
        "created": created,
        "has_more": len(events) >= max(1, min(int(limit), 500)),
        "index_key": cursor.index_key,
    }


def _incoming_attachments(message: dict[str, Any]) -> list[tuple[str, str]]:
    message_id = str(message.get("MessageId") or "").strip()
    result: list[tuple[str, str]] = []
    for entity in message.get("Entities") or []:
        info = entity.get("DocumentInfo") or {}
        direction = str(info.get("DocumentDirection") or "").lower()
        entity_id = str(entity.get("EntityId") or info.get("EntityId") or "").strip()
        if (
            message_id
            and entity_id
            and str(entity.get("EntityType") or "") == "Attachment"
            and direction == "inbound"
        ):
            result.append((message_id, entity_id))
    return result


async def _refresh_incoming_docflows(
    session: AsyncSession,
    *,
    client: DiadocClient,
    documents: list[DiadocIncomingDocument],
) -> int:
    if not documents:
        return 0
    payload = await client.get_docflows(
        box_id_guid=documents[0].box_id_guid,
        document_ids=[(doc.message_id, doc.entity_id) for doc in documents],
    )
    by_id = {
        (
            str((row.get("DocumentId") or {}).get("MessageId") or ""),
            str((row.get("DocumentId") or {}).get("EntityId") or ""),
        ): row
        for row in payload.get("Documents") or []
    }
    updated = 0
    for document in documents:
        row = by_id.get((document.message_id, document.entity_id))
        if row is None:
            continue
        state = derive_docflow_state(row, current_status=str(document.status or "synced"))
        document.docflow_is_finished = state["is_finished"]
        document.docflow_status_severity = state["docflow_status_severity"]
        document.docflow_status_text = state["docflow_status_text"]
        document.recipient_response_status = state["recipient_response_status"]
        document.revocation_status = state["revocation_status"]
        document.docflow_checked_at = state["checked_at"]
        document.last_docflow_payload = row
        session.add(document)
        updated += 1
    await session.flush()
    return updated


async def _refresh_outgoing_by_message(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    message_id: str | None,
) -> int:
    """Обновляет статусы наших исходящих документов из сообщения события."""
    if not message_id:
        return 0
    from dz_fastapi.services.diadoc_status import refresh_outgoing_document_status

    documents = list(
        (
            await session.execute(
                select(DiadocOutgoingDocument).where(
                    DiadocOutgoingDocument.environment == environment,
                    DiadocOutgoingDocument.from_box_id_guid == box_id_guid,
                    DiadocOutgoingDocument.message_id == message_id,
                    DiadocOutgoingDocument.entity_id.is_not(None),
                    DiadocOutgoingDocument.is_draft.is_(False),
                )
            )
        ).scalars()
    )
    for document in documents:
        await refresh_outgoing_document_status(session, client=client, document=document)
    return len(documents)


# После стольких неудач событие перестаёт повторяться само: постоянная
# ошибка (битый документ, удалённое сообщение) иначе крутится бесконечно и
# каждый запуск тратит запросы к Диадоку. Вернуть в работу — retry_failed_events.
MAX_EVENT_ATTEMPTS = 8


async def event_queue_summary(
    session: AsyncSession, *, environment: str, box_id_guid: str | None
) -> dict[str, Any]:
    """Состояние ленты для экрана настроек: курсор и очередь событий."""
    summary: dict[str, Any] = {
        "initialized": False,
        "last_polled_at": None,
        "pending": 0,
        "error": 0,
        "failed": 0,
        "failed_examples": [],
    }
    if not box_id_guid:
        return summary
    cursor = await get_event_cursor(
        session, environment=environment, box_id_guid=box_id_guid
    )
    if cursor is not None:
        summary["initialized"] = cursor.initialized_at is not None
        summary["last_polled_at"] = cursor.last_polled_at
    scope = (
        DiadocBoxEvent.environment == environment,
        DiadocBoxEvent.box_id_guid == box_id_guid,
    )
    counts = (
        await session.execute(
            select(DiadocBoxEvent.status, func.count())
            .where(*scope, DiadocBoxEvent.status.in_(("pending", "error", "failed")))
            .group_by(DiadocBoxEvent.status)
        )
    ).all()
    for status_name, count in counts:
        summary[str(status_name)] = int(count)
    failed_rows = (
        await session.execute(
            select(DiadocBoxEvent)
            .where(*scope, DiadocBoxEvent.status == "failed")
            .order_by(DiadocBoxEvent.id.desc())
            .limit(5)
        )
    ).scalars()
    summary["failed_examples"] = [
        {
            "event_id": row.event_id,
            "message_id": row.message_id,
            "attempts": int(row.attempt_count or 0),
            "error": row.last_error,
        }
        for row in failed_rows
    ]
    return summary


async def retry_failed_events(
    session: AsyncSession, *, environment: str, box_id_guid: str
) -> int:
    """Возвращает остановленные события в очередь с обнулённым счётчиком."""
    rows = list(
        (
            await session.execute(
                select(DiadocBoxEvent).where(
                    DiadocBoxEvent.environment == environment,
                    DiadocBoxEvent.box_id_guid == box_id_guid,
                    DiadocBoxEvent.status == "failed",
                )
            )
        ).scalars()
    )
    for row in rows:
        row.status = "pending"
        row.attempt_count = 0
        session.add(row)
    await session.flush()
    return len(rows)


async def process_pending_events(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    download_content: bool,
    process_supplier_message: bool,
    limit: int = 100,
) -> dict[str, Any]:
    rows = list(
        (
            await session.execute(
                select(DiadocBoxEvent)
                .where(
                    DiadocBoxEvent.environment == environment,
                    DiadocBoxEvent.box_id_guid == box_id_guid,
                    DiadocBoxEvent.status.in_(("pending", "error")),
                )
                .order_by(DiadocBoxEvent.id)
                .limit(limit)
            )
        ).scalars()
    )
    result = {
        "processed": 0,
        "failed": 0,
        "gave_up": 0,
        "documents": 0,
        "docflows": 0,
        "outgoing": 0,
        "errors": [],
    }
    for row in rows:
        row.attempt_count += 1
        try:
            kind, message_id, body = _event_message(row.payload or {})
            documents: list[DiadocIncomingDocument] = []
            if kind == "message":
                for current_message_id, entity_id in _incoming_attachments(body):
                    synced = await sync_diadoc_incoming_document_by_id(
                        session,
                        client=client,
                        environment=environment,
                        box_id_guid=box_id_guid,
                        message_id=current_message_id,
                        entity_id=entity_id,
                        event_index_key=row.index_key,
                        download_content=download_content,
                        register_supplier_message=process_supplier_message,
                        process_supplier_message=process_supplier_message,
                    )
                    document = await session.get(DiadocIncomingDocument, synced["document_id"])
                    if document is not None:
                        documents.append(document)
                    result["documents"] += 1
            elif kind == "patch" and message_id:
                documents = list(
                    (
                        await session.execute(
                            select(DiadocIncomingDocument).where(
                                DiadocIncomingDocument.environment == environment,
                                DiadocIncomingDocument.box_id_guid == box_id_guid,
                                DiadocIncomingDocument.message_id == message_id,
                            )
                        )
                    ).scalars()
                )
            result["docflows"] += await _refresh_incoming_docflows(
                session, client=client, documents=documents
            )
            result["outgoing"] += await _refresh_outgoing_by_message(
                session,
                client=client,
                environment=environment,
                box_id_guid=box_id_guid,
                message_id=message_id,
            )
            row.status = "processed"
            row.processed_at = now_moscow()
            row.last_error = None
            result["processed"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to process Diadoc box event %s", row.event_id)
            gave_up = int(row.attempt_count or 0) >= MAX_EVENT_ATTEMPTS
            row.status = "failed" if gave_up else "error"
            row.last_error = str(exc)[:2000]
            result["failed"] += 1
            if gave_up:
                result["gave_up"] += 1
                result["errors"].append(
                    f"{row.event_id}: {exc} (попыток: {row.attempt_count}, "
                    "повторы остановлены)"
                )
            else:
                result["errors"].append(f"{row.event_id}: {exc}")
        session.add(row)
        await session.flush()
    return result


async def sync_event_feed(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    download_content: bool,
    process_supplier_message: bool,
    page_size: int = 100,
    max_pages: int = 20,
) -> dict[str, Any]:
    cursor = await get_event_cursor(session, environment=environment, box_id_guid=box_id_guid)
    if cursor is None or cursor.initialized_at is None:
        return {"initialized": False, "note": "event cursor is not initialized"}
    summary = {"initialized": True, "received": 0, "created": 0, "pages": 0}
    for _ in range(max_pages):
        page = await fetch_event_page(session, client=client, cursor=cursor, limit=page_size)
        summary["pages"] += 1
        summary["received"] += page["received"]
        summary["created"] += page["created"]
        await session.commit()
        if not page["has_more"]:
            break
    processed = await process_pending_events(
        session,
        client=client,
        environment=environment,
        box_id_guid=box_id_guid,
        download_content=download_content,
        process_supplier_message=process_supplier_message,
        limit=page_size * max_pages,
    )
    summary.update(processed)
    return summary
