"""Статусы исходящих документов по каноническому GetDocflows V5."""

from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocApiError, DiadocClient
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.services.diadoc_docflow import derive_docflow_state
from dz_fastapi.services.diadoc_transport import extract_transport_status

logger = logging.getLogger("dz_fastapi")

# Внутренние статусы, после которых опрашивать документ больше не нужно.
TERMINAL_OUTGOING_STATUSES = {"completed", "rejected", "revoked"}

DEFAULT_STATUS_REFRESH_LIMIT = 100


def _extract_transformed_message_id(
    payload: dict[str, Any],
) -> Optional[str]:
    """Черновик, отправленный из веба Диадока, превращается в новое
    сообщение — если API вернул его id, начинаем следить за ним."""
    for key in (
        "TransformedToMessageId",
        "TransformedToMessageIdGuid",
    ):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return None


async def refresh_outgoing_document_status(
    session: AsyncSession,
    *,
    client: DiadocClient,
    document: DiadocOutgoingDocument,
) -> DiadocOutgoingDocument:
    """Обновляет статус исходящего по GetDocflows V5.

    GetDocument остаётся только для ещё не отправленного черновика, чтобы
    обнаружить его преобразование в сообщение после отправки из веб-Диадока.
    """
    if not document.message_id or not document.entity_id:
        raise ValueError("У документа нет message_id/entity_id — статус недоступен")
    if document.is_draft:
        draft_payload = await client.get_document(
            box_id_guid=document.from_box_id_guid,
            message_id=document.message_id,
            entity_id=document.entity_id,
        )
        transformed_message_id = _extract_transformed_message_id(draft_payload)
        if not transformed_message_id:
            document.status = "draft"
            document.status_checked_at = now_moscow()
            document.last_status_payload = draft_payload
            session.add(document)
            await session.commit()
            await session.refresh(document)
            return document
        document.message_id = transformed_message_id
        document.is_draft = False

    response = await client.get_docflows(
        box_id_guid=document.from_box_id_guid,
        document_ids=[(document.message_id, document.entity_id)],
    )
    rows = list(response.get("Documents") or [])
    if not rows:
        raise DiadocApiError(404, "GetDocflows V5 не вернул документ")
    payload = rows[0]
    fields = derive_docflow_state(payload, current_status=str(document.status or "sent"))
    document.status = fields["status"]
    document.docflow_is_finished = fields["is_finished"]
    document.docflow_status_severity = fields["docflow_status_severity"]
    document.docflow_status_text = fields["docflow_status_text"]
    document.recipient_response_status = fields["recipient_response_status"]
    document.revocation_status = fields["revocation_status"]
    # Статус ГИС ЭПД перезаписываем только когда он реально пришёл: у
    # обычных УПД его нет, и затирать ранее полученный статус нельзя.
    transport = extract_transport_status(payload) or {}
    if transport.get("status_named_id") or transport.get("status_text"):
        document.transport_status_named_id = transport.get("status_named_id")
        document.transport_status_type = transport.get("status_type")
        document.transport_status_text = transport.get("status_text")
        document.transport_mintrans_id = transport.get("mintrans_id")
        document.transport_carriage_id = transport.get("carriage_id")
    if fields["delivered_at"] is not None:
        document.delivered_at = fields["delivered_at"]
    document.status_checked_at = fields["checked_at"]
    document.last_status_payload = payload
    session.add(document)
    await session.commit()
    await session.refresh(document)
    return document


async def refresh_diadoc_outgoing_statuses(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    limit: int = DEFAULT_STATUS_REFRESH_LIMIT,
) -> dict[str, Any]:
    """Обновляет статусы незавершённых исходящих документов.

    Сначала самые давно не проверявшиеся. Ошибка по одному документу не
    останавливает остальные.
    """
    stmt = (
        select(DiadocOutgoingDocument)
        .where(
            DiadocOutgoingDocument.environment == environment,
            DiadocOutgoingDocument.message_id.is_not(None),
            DiadocOutgoingDocument.entity_id.is_not(None),
            DiadocOutgoingDocument.status.not_in(TERMINAL_OUTGOING_STATUSES),
        )
        .order_by(
            DiadocOutgoingDocument.status_checked_at.asc().nulls_first(),
            DiadocOutgoingDocument.id.asc(),
        )
        .limit(max(1, int(limit)))
    )
    documents = list((await session.execute(stmt)).scalars().all())
    result: dict[str, Any] = {
        "checked": 0,
        "updated": 0,
        "completed": 0,
        "rejected": 0,
        "revoked": 0,
        "technical_error": 0,
        "errors": [],
    }
    for document in documents:
        previous_status = str(document.status or "")
        result["checked"] += 1
        try:
            document = await refresh_outgoing_document_status(
                session,
                client=client,
                document=document,
            )
        except DiadocApiError as exc:
            await session.rollback()
            if exc.status_code == 404:
                # 404 не является отказом контрагента. Идентификатор мог
                # измениться после отправки черновика или быть временно
                # недоступен, поэтому оставляем документ для повторной проверки.
                document.status = "technical_error"
                document.docflow_status_text = "Документ временно не найден в Диадоке"
                document.error_details = "HTTP 404 при проверке статуса; будет проверен повторно"
                document.status_checked_at = now_moscow()
                session.add(document)
                await session.commit()
                result["technical_error"] += 1
            result["errors"].append(f"#{document.id}: HTTP {exc.status_code} {exc.detail[:200]}")
            continue
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            logger.exception(
                "Failed to refresh Diadoc outgoing status for #%s",
                document.id,
            )
            result["errors"].append(f"#{document.id}: {exc}")
            continue
        new_status = str(document.status or "")
        if new_status != previous_status:
            result["updated"] += 1
        if new_status in result:
            result[new_status] += 1
    return result
