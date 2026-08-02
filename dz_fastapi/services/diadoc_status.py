"""Отслеживание статусов исходящих документов Диадока.

После отправки (PostMessage) документ живёт своей жизнью: доставка,
подпись контрагента, отказ, аннулирование. Регламент опрашивает
GetDocument по каждому незавершённому исходящему документу и обновляет
внутренний статус + текст статуса как в вебе Диадока.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocApiError, DiadocClient
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.services.diadoc_documents import _ticks_to_datetime
from dz_fastapi.services.diadoc_transport import extract_transport_status

logger = logging.getLogger("dz_fastapi")

# Внутренние статусы, после которых опрашивать документ больше не нужно.
TERMINAL_OUTGOING_STATUSES = {"completed", "rejected", "revoked"}

DEFAULT_STATUS_REFRESH_LIMIT = 100


def derive_outgoing_status_fields(
    payload: dict[str, Any],
    *,
    current_status: str,
    is_draft: bool,
) -> dict[str, Any]:
    """Извлекает поля статуса из ответа GetDocument (чистая функция).

    Возвращает dict с ключами: status, docflow_status_severity,
    docflow_status_text, recipient_response_status, revocation_status,
    delivered_at, а для перевозочных документов — ещё и статус ГИС ЭПД
    (transport_*). У неперевозочных документов transport_* остаются None.
    """
    docflow = payload.get("DocflowStatus") or {}
    primary = docflow.get("PrimaryStatus") or {}
    severity = str(primary.get("Severity") or "").strip() or None
    status_text = str(primary.get("StatusText") or "").strip() or None
    recipient_response = (
        str(payload.get("RecipientResponseStatus") or "").strip() or None
    )
    revocation = str(payload.get("RevocationStatus") or "").strip() or None
    delivered_at = _ticks_to_datetime(payload.get("DeliveryTimestampTicks"))

    text_lower = (status_text or "").lower()
    recipient_lower = (recipient_response or "").lower()
    revocation_lower = (revocation or "").lower()
    severity_lower = (severity or "").lower()

    if (
        "revocationaccepted" in revocation_lower
        or "аннулирован" in text_lower
    ):
        status = "revoked"
    elif (
        "отказ" in text_lower
        or "reject" in recipient_lower
        or severity_lower == "error"
    ):
        status = "rejected"
    elif (
        severity_lower == "success"
        or "заверш" in text_lower
        or "signed" in recipient_lower
        or "signature" in recipient_lower
    ):
        status = "completed"
    elif delivered_at is not None:
        status = "delivered"
    elif is_draft:
        status = "draft"
    else:
        status = current_status or "sent"

    transport = extract_transport_status(payload) or {}

    return {
        "status": status,
        "docflow_status_severity": severity,
        "docflow_status_text": status_text,
        "recipient_response_status": recipient_response,
        "revocation_status": revocation,
        "delivered_at": delivered_at,
        "transport_status_named_id": transport.get("status_named_id"),
        "transport_status_type": transport.get("status_type"),
        "transport_status_text": transport.get("status_text"),
        "transport_mintrans_id": transport.get("mintrans_id"),
        "transport_carriage_id": transport.get("carriage_id"),
    }


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
    """Обновляет статус одного исходящего документа по GetDocument."""
    if not document.message_id or not document.entity_id:
        raise ValueError(
            "У документа нет message_id/entity_id — статус недоступен"
        )
    payload = await client.get_document(
        box_id_guid=document.from_box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
    )
    transformed_message_id = _extract_transformed_message_id(payload)
    if (
        transformed_message_id
        and transformed_message_id != document.message_id
    ):
        document.message_id = transformed_message_id
        document.is_draft = False

    fields = derive_outgoing_status_fields(
        payload,
        current_status=str(document.status or "sent"),
        is_draft=bool(document.is_draft),
    )
    if fields["status"] not in ("draft",) and document.is_draft:
        # Документ виден с доставкой/подписью — черновик уже отправлен.
        document.is_draft = False
    document.status = fields["status"]
    document.docflow_status_severity = fields["docflow_status_severity"]
    document.docflow_status_text = fields["docflow_status_text"]
    document.recipient_response_status = fields["recipient_response_status"]
    document.revocation_status = fields["revocation_status"]
    # Статус ГИС ЭПД перезаписываем только когда он реально пришёл: у
    # обычных УПД его нет, и затирать ранее полученный статус нельзя.
    if fields["transport_status_named_id"] or fields["transport_status_text"]:
        document.transport_status_named_id = fields[
            "transport_status_named_id"
        ]
        document.transport_status_type = fields["transport_status_type"]
        document.transport_status_text = fields["transport_status_text"]
        document.transport_mintrans_id = fields["transport_mintrans_id"]
        document.transport_carriage_id = fields["transport_carriage_id"]
    if fields["delivered_at"] is not None:
        document.delivered_at = fields["delivered_at"]
    document.status_checked_at = now_moscow()
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
            DiadocOutgoingDocument.status.not_in(
                TERMINAL_OUTGOING_STATUSES
            ),
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
                # Черновик удалили в вебе Диадока — фиксируем и больше
                # не опрашиваем.
                document.status = "rejected"
                document.docflow_status_text = (
                    "Документ не найден в Диадоке (удалён?)"
                )
                document.status_checked_at = now_moscow()
                session.add(document)
                await session.commit()
            result["errors"].append(
                f"#{document.id}: HTTP {exc.status_code} {exc.detail[:200]}"
            )
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
