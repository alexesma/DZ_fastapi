"""Конвейер исходящих УПД: «провели реализацию → черновик УПД ждёт подписи».

Раньше каждый шаг запускался руками: провести, сформировать УПД, подписать.
Теперь после проведения черновик формализованного УПД создаётся сам и встаёт
в очередь «к подписи»; сотруднику остаётся подписать (код из SMS вводит
человек). Реализации, по которым черновик создать нельзя, остаются в очереди
с причиной.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.diadoc import DiadocOutgoingDocument
from dz_fastapi.models.inventory import ShipmentDocument, ShipmentDocumentStatus
from dz_fastapi.models.settings import DiadocIntegrationSettings
from dz_fastapi.services.diadoc_oauth import normalize_diadoc_environment
from dz_fastapi.services.diadoc_outgoing import (
    build_formalized_diadoc_payload_from_shipment,
    build_shipment_formalized_readiness,
    post_diadoc_outgoing_document,
    resolve_diadoc_box_for_customer,
)

logger = logging.getLogger("dz_fastapi")

SOURCE_SHIPMENT = "shipment_document"
_MAX_REASON = 1000


class UpdDraftBlocked(ValueError):
    """Черновик УПД создать нельзя; текст — причина для сотрудника."""


def _has_outgoing_document():
    return exists().where(
        DiadocOutgoingDocument.source_type == SOURCE_SHIPMENT,
        DiadocOutgoingDocument.source_id == ShipmentDocument.id,
    )


async def create_shipment_upd_draft(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    shipment_id: int,
) -> DiadocOutgoingDocument:
    """Создаёт в Диадоке черновик формализованного УПД по реализации."""
    from dz_fastapi.services.marking_shipment_check import shipment_codes_gis_blockers

    from_box = str(integration.box_id_guid or "").strip()
    if not from_box:
        raise UpdDraftBlocked("Не выбран ящик Диадока организации")
    readiness = await build_shipment_formalized_readiness(
        session, shipment_id=shipment_id, integration=integration
    )
    if not readiness["ready_formalized"]:
        raise UpdDraftBlocked(
            "; ".join(readiness["missing_required_fields"])
            or "Реализация не готова к формализованному УПД"
        )
    gis_blockers = await shipment_codes_gis_blockers(session, shipment_id)
    if gis_blockers:
        raise UpdDraftBlocked("; ".join(gis_blockers[:10]))
    payload = await build_formalized_diadoc_payload_from_shipment(
        session,
        client=client,
        integration=integration,
        shipment_id=shipment_id,
    )
    to_box = str(payload.get("to_box_id_guid") or "").strip() or (
        await resolve_diadoc_box_for_customer(
            session, customer_id=int(payload["customer_id"])
        )
    )
    if not to_box:
        raise UpdDraftBlocked("У клиента нет ящика Диадока — документ по ЭДО не отправить")
    return await post_diadoc_outgoing_document(
        session,
        client=client,
        environment=normalize_diadoc_environment(integration.environment),
        from_box_id_guid=from_box,
        to_box_id_guid=to_box,
        customer_id=int(payload["customer_id"]),
        file_name=str(payload["file_name"]),
        content_base64=str(payload["content_base64"]),
        signature_base64=None,
        comment=None,
        need_recipient_signature=True,
        need_receipt=True,
        send_mode="draft",
        type_named_id=str(payload.get("type_named_id") or "").strip()
        or "UniversalTransferDocument",
        document_function=payload.get("document_function"),
        document_version=payload.get("document_version"),
        document_number=payload.get("document_number"),
        document_date=payload.get("document_date"),
        metadata=payload.get("metadata"),
        source_type=str(payload["source_type"]),
        source_id=int(payload["source_id"]),
    )


async def auto_draft_posted_shipments(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    limit: int = 20,
) -> dict[str, Any]:
    """Создаёт черновики УПД по проведённым реализациям без документа ЭДО.

    Берутся реализации, проведённые после включения конвейера. Причина
    неудачи записывается в реализацию и видна в очереди; попытка повторится
    на следующем проходе — после исправления данных документ уйдёт сам.
    """
    result: dict[str, Any] = {"created": 0, "blocked": 0, "errors": 0}
    since = integration.outbound_auto_draft_from
    if not integration.outbound_auto_draft_enabled or since is None:
        result["skipped"] = "disabled"
        return result
    shipment_ids = list(
        (
            await session.execute(
                select(ShipmentDocument.id)
                .where(
                    ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
                    ShipmentDocument.posted_at >= since,
                    ShipmentDocument.customer_id.is_not(None),
                    ~_has_outgoing_document(),
                )
                .order_by(
                    ShipmentDocument.edo_auto_checked_at.asc().nulls_first(),
                    ShipmentDocument.id,
                )
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    for shipment_id in shipment_ids:
        reason: Optional[str] = None
        try:
            await create_shipment_upd_draft(
                session, client=client, integration=integration, shipment_id=shipment_id
            )
            result["created"] += 1
        except UpdDraftBlocked as exc:
            reason = str(exc)
            result["blocked"] += 1
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            reason = f"Ошибка Диадока: {exc}"
            result["errors"] += 1
            logger.warning("Auto UPD draft for shipment %s failed: %s", shipment_id, exc)
        shipment = await session.get(ShipmentDocument, shipment_id)
        if shipment is not None:
            shipment.edo_auto_error = reason[:_MAX_REASON] if reason else None
            shipment.edo_auto_checked_at = now_moscow()
        await session.commit()
    return result


async def signing_queue(session: AsyncSession, *, limit: int = 200) -> dict[str, Any]:
    """Очередь «к подписи»: готовые черновики и реализации, которым что-то мешает."""
    drafts = list(
        (
            await session.execute(
                select(DiadocOutgoingDocument)
                .where(
                    DiadocOutgoingDocument.source_type == SOURCE_SHIPMENT,
                    DiadocOutgoingDocument.is_draft.is_(True),
                    DiadocOutgoingDocument.status == "draft",
                )
                .options(selectinload(DiadocOutgoingDocument.customer))
                .order_by(DiadocOutgoingDocument.id)
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    blocked = list(
        (
            await session.execute(
                select(ShipmentDocument)
                .where(
                    ShipmentDocument.status == ShipmentDocumentStatus.POSTED,
                    ShipmentDocument.edo_auto_error.is_not(None),
                    ~_has_outgoing_document(),
                )
                .order_by(ShipmentDocument.id)
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return {
        "drafts": [
            {
                "id": row.id,
                "shipment_id": row.source_id,
                "document_number": row.document_number,
                "document_date": row.document_date,
                "customer_id": row.customer_id,
                "customer_name": getattr(row.__dict__.get("customer"), "name", None),
                "file_name": row.file_name,
                "created_at": getattr(row, "created_at", None),
            }
            for row in drafts
        ],
        "blocked": [
            {
                "shipment_id": row.id,
                "document_number": row.doc_number,
                "posted_at": row.posted_at,
                "customer_id": row.customer_id,
                "customer_name": getattr(row.customer, "name", None),
                "reason": row.edo_auto_error,
                "checked_at": row.edo_auto_checked_at,
            }
            for row in blocked
        ],
    }
