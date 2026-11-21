"""Исходящие документы из Диадока, созданные не в нашей системе.

Часть документов уходит контрагентам из веб-Диадока или из 1С напрямую.
Лента событий отдаёт их как ``Outbound``, но раньше мы разбирали только
``Inbound``, а по исходящим обновляли статусы лишь у документов, которые сами
отправили. Здесь такие документы заводятся в ``DiadocOutgoingDocument`` с
``source_type="diadoc_external"``, подтягиваются их файл и статус документооборота.
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.services.diadoc_documents import (
    _build_diadoc_storage_relative_path,
    _parse_document_date,
    _ticks_to_datetime,
    _write_content_to_relative_path,
    resolve_provider_by_diadoc_counteragent_box,
)
from dz_fastapi.services.diadoc_outgoing import resolve_customer_by_diadoc_counteragent_box
from dz_fastapi.services.diadoc_status import refresh_outgoing_document_status

logger = logging.getLogger("dz_fastapi")

EXTERNAL_SOURCE_TYPE = "diadoc_external"


def external_import_enabled() -> bool:
    return os.getenv("DIADOC_IMPORT_EXTERNAL_OUTBOUND", "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def outbound_attachments(message: dict[str, Any]) -> list[tuple[str, str]]:
    """Пары (MessageId, EntityId) исходящих вложений из сообщения события."""
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
            and direction == "outbound"
        ):
            result.append((message_id, entity_id))
    return result


async def import_external_outgoing_document(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    message_id: str,
    entity_id: str,
    raw: dict[str, Any] | None = None,
    download_content: bool = True,
) -> dict[str, Any]:
    """Заводит исходящий документ, которого нет у нас. Уже известный не трогает."""
    existing = (
        await session.execute(
            select(DiadocOutgoingDocument).where(
                DiadocOutgoingDocument.environment == environment,
                DiadocOutgoingDocument.from_box_id_guid == box_id_guid,
                DiadocOutgoingDocument.entity_id == entity_id,
            )
        )
    ).scalars().first()
    if existing is not None:
        return {"created": False, "document_id": existing.id, "reason": "known"}

    if raw is None:
        raw = await client.get_document(
            box_id_guid=box_id_guid, message_id=message_id, entity_id=entity_id
        )
    direction = str(raw.get("DocumentDirection") or "").lower()
    if direction and direction != "outbound":
        return {"created": False, "document_id": None, "reason": "not_outbound"}

    counteragent = str(raw.get("CounteragentBoxId") or "").strip()
    customer = await resolve_customer_by_diadoc_counteragent_box(session, counteragent)
    provider_id = await resolve_provider_by_diadoc_counteragent_box(
        session, counteragent_box_id=counteragent
    )
    document = DiadocOutgoingDocument(
        environment=environment,
        from_box_id_guid=box_id_guid,
        to_box_id_guid=counteragent,
        customer_id=customer.id if customer is not None else None,
        provider_id=provider_id,
        source_type=EXTERNAL_SOURCE_TYPE,
        source_id=None,
        source_version=1,
        type_named_id=str(raw.get("TypeNamedId") or "Nonformalized"),
        document_function=raw.get("Function") or None,
        document_version=raw.get("Version") or None,
        file_name=str(raw.get("FileName") or entity_id)[:500],
        document_number=str(raw.get("DocumentNumber") or "")[:120] or None,
        document_date=_parse_document_date(raw.get("DocumentDate")),
        local_file_path="",
        is_draft=False,
        message_id=message_id,
        entity_id=entity_id,
        status="sent",
        sent_at=_ticks_to_datetime(raw.get("SendTimestampTicks")),
        metadata_json={"external": True},
        raw_response=raw,
    )
    session.add(document)
    await session.flush()

    if download_content:
        try:
            content = await client.get_entity_content(
                box_id_guid=box_id_guid, message_id=message_id, entity_id=entity_id
            )
            relative_path = _build_diadoc_storage_relative_path(
                environment=environment,
                box_id_guid=box_id_guid,
                message_id=message_id,
                entity_id=entity_id,
                filename=document.file_name,
            )
            await _write_content_to_relative_path(relative_path, content)
            document.local_file_path = relative_path
            document.content_sha256 = hashlib.sha256(content).hexdigest()
        except Exception as exc:  # noqa: BLE001 - файл подтянется позже
            logger.warning("Diadoc external outgoing content failed %s: %s", entity_id, exc)
            document.error_details = f"Файл не загружен: {exc}"[:2000]
    session.add(document)
    await session.commit()
    try:
        await refresh_outgoing_document_status(session, client=client, document=document)
    except Exception as exc:  # noqa: BLE001 - статус обновит плановая синхронизация
        logger.warning("Diadoc external outgoing status failed %s: %s", entity_id, exc)
        document.status_checked_at = now_moscow()
        session.add(document)
        await session.commit()
    return {"created": True, "document_id": document.id, "reason": "created"}


async def sync_external_outgoing_documents(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    from_document_date=None,
    to_document_date=None,
    count: int = 50,
    max_pages: int = 20,
    download_content: bool = True,
) -> dict[str, Any]:
    """Дозагрузка истории исходящих через GetDocuments (Any.Outbound)."""
    result: dict[str, Any] = {
        "pages": 0,
        "seen": 0,
        "created": 0,
        "known": 0,
        "errors": [],
    }
    after_index_key = None
    for _ in range(max(1, max_pages)):
        payload = await client.get_documents(
            box_id_guid=box_id_guid,
            filter_category="Any.Outbound",
            count=max(1, min(int(count), 100)),
            after_index_key=after_index_key,
            from_document_date=from_document_date,
            to_document_date=to_document_date,
            sort_direction="Descending",
        )
        documents = list(payload.get("Documents") or [])
        result["pages"] += 1
        for raw in documents:
            message_id = str(raw.get("MessageId") or "").strip()
            entity_id = str(raw.get("EntityId") or "").strip()
            if not message_id or not entity_id:
                continue
            result["seen"] += 1
            try:
                item = await import_external_outgoing_document(
                    session,
                    client=client,
                    environment=environment,
                    box_id_guid=box_id_guid,
                    message_id=message_id,
                    entity_id=entity_id,
                    raw=raw,
                    download_content=download_content,
                )
            except Exception as exc:  # noqa: BLE001
                await session.rollback()
                result["errors"].append(f"{message_id}/{entity_id}: {exc}"[:300])
                continue
            result["created" if item["created"] else "known"] += 1
        after_index_key = str(documents[-1].get("IndexKey") or "") if documents else ""
        if not payload.get("HasMoreResults") or not after_index_key:
            break
    return result
