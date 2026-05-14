import hashlib
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiofiles
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.diadoc import DiadocIncomingDocument
from dz_fastapi.models.partner import (
    ProviderExternalReference,
    SupplierOrderAttachment,
    SupplierOrderMessage,
    SupplierResponseConfig,
)
from dz_fastapi.services.supplier_order_responses import process_stored_supplier_document_message

DIADOC_DOCUMENTS_DIR = os.getenv(
    "DIADOC_DOCUMENTS_DIR",
    "uploads/diadoc_documents",
)
DIADOC_PROVIDER_SOURCE_SYSTEMS = (
    "DIADOC_BOX",
    "DIADOC_COUNTERAGENT_BOX",
)
_INVALID_FILENAME_CHARS_RE = re.compile(r"[^A-Za-z0-9._-]+")
_DOTNET_TICKS_EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)


def _safe_filename(filename: str | None) -> str:
    raw = str(filename or "").strip()
    if not raw:
        return "document.bin"
    sanitized = _INVALID_FILENAME_CHARS_RE.sub("_", raw)
    return sanitized[:240] or "document.bin"


def _ticks_to_datetime(value: object) -> datetime | None:
    try:
        ticks = int(value)
    except (TypeError, ValueError):
        return None
    if ticks <= 0:
        return None
    try:
        dt = _DOTNET_TICKS_EPOCH + timedelta(microseconds=ticks / 10)
    except OverflowError:
        return None
    return dt.astimezone()


def _parse_document_date(value: object):
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _build_diadoc_storage_relative_path(
    *,
    environment: str,
    box_id_guid: str,
    message_id: str,
    filename: str | None,
) -> str:
    safe_box = _safe_filename(box_id_guid)
    safe_message = _safe_filename(message_id)
    safe_name = _safe_filename(filename)
    date_str = now_moscow().strftime("%Y%m%d")
    return os.path.join(
        DIADOC_DOCUMENTS_DIR,
        environment,
        safe_box,
        date_str,
        f"{safe_message}_{safe_name}",
    )


async def _write_content_to_relative_path(
    relative_path: str,
    content: bytes,
) -> str:
    abs_path = os.path.abspath(relative_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    async with aiofiles.open(abs_path, "wb") as fh:
        await fh.write(content)
    return relative_path


async def get_diadoc_incoming_document(
    session: AsyncSession,
    *,
    document_id: int,
) -> Optional[DiadocIncomingDocument]:
    stmt = (
        select(DiadocIncomingDocument)
        .options(
            selectinload(DiadocIncomingDocument.provider),
            selectinload(
                DiadocIncomingDocument.supplier_order_message
            ).selectinload(SupplierOrderMessage.receipts),
        )
        .where(DiadocIncomingDocument.id == document_id)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_diadoc_incoming_documents(
    session: AsyncSession,
    *,
    document_id: int | None = None,
    provider_id: int | None = None,
    registered_only: bool | None = None,
    limit: int = 100,
) -> list[DiadocIncomingDocument]:
    stmt = (
        select(DiadocIncomingDocument)
        .options(
            selectinload(DiadocIncomingDocument.provider),
            selectinload(
                DiadocIncomingDocument.supplier_order_message
            ).selectinload(SupplierOrderMessage.receipts),
        )
        .order_by(
            DiadocIncomingDocument.document_date.desc().nullslast(),
            DiadocIncomingDocument.id.desc(),
        )
        .limit(max(1, min(int(limit or 100), 300)))
    )
    if document_id is not None:
        stmt = stmt.where(DiadocIncomingDocument.id == document_id)
    if provider_id is not None:
        stmt = stmt.where(DiadocIncomingDocument.provider_id == provider_id)
    if registered_only is True:
        stmt = stmt.where(
            DiadocIncomingDocument.supplier_order_message_id.is_not(None)
        )
    elif registered_only is False:
        stmt = stmt.where(
            DiadocIncomingDocument.supplier_order_message_id.is_(None)
        )
    return list((await session.execute(stmt)).scalars().all())


async def resolve_provider_by_diadoc_counteragent_box(
    session: AsyncSession,
    *,
    counteragent_box_id: str | None,
) -> int | None:
    value = str(counteragent_box_id or "").strip()
    if not value:
        return None
    stmt = (
        select(ProviderExternalReference)
        .where(
            ProviderExternalReference.is_active.is_(True),
            ProviderExternalReference.source_system.in_(
                DIADOC_PROVIDER_SOURCE_SYSTEMS
            ),
            or_(
                ProviderExternalReference.external_supplier_name == value,
                ProviderExternalReference.external_supplier_name
                == value.lower(),
                ProviderExternalReference.external_supplier_name
                == value.upper(),
            ),
        )
        .order_by(ProviderExternalReference.id.asc())
    )
    reference = (await session.execute(stmt)).scalars().first()
    return int(reference.provider_id) if reference is not None else None


async def ensure_diadoc_document_content(
    session: AsyncSession,
    *,
    client: DiadocClient,
    document: DiadocIncomingDocument,
) -> DiadocIncomingDocument:
    if document.local_file_path and os.path.exists(
        os.path.abspath(document.local_file_path)
    ):
        return document
    content = await client.get_entity_content(
        box_id_guid=document.box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
    )
    relative_path = _build_diadoc_storage_relative_path(
        environment=document.environment,
        box_id_guid=document.box_id_guid,
        message_id=document.message_id,
        filename=document.file_name,
    )
    await _write_content_to_relative_path(relative_path, content)
    document.local_file_path = relative_path
    document.content_sha256 = hashlib.sha256(content).hexdigest()
    document.synced_at = now_moscow()
    session.add(document)
    await session.commit()
    await session.refresh(document)
    return document


async def _pick_default_supplier_response_config_id(
    session: AsyncSession,
    *,
    provider_id: int,
) -> int | None:
    stmt = (
        select(SupplierResponseConfig)
        .where(
            SupplierResponseConfig.provider_id == provider_id,
            SupplierResponseConfig.is_active.is_(True),
            SupplierResponseConfig.response_type == "file",
            SupplierResponseConfig.file_payload_type == "document",
        )
        .order_by(SupplierResponseConfig.id.asc())
    )
    configs = list((await session.execute(stmt)).scalars().all())
    if len(configs) == 1:
        return int(configs[0].id)
    return None


async def register_diadoc_document_as_supplier_message(
    session: AsyncSession,
    *,
    document: DiadocIncomingDocument,
    provider_id: int | None = None,
    response_config_id: int | None = None,
) -> SupplierOrderMessage:
    target_provider_id = int(provider_id or document.provider_id or 0)
    if target_provider_id <= 0:
        raise ValueError(
            "Provider is not resolved. "
            "Bind provider via external reference or pass provider_id."
        )
    if not document.local_file_path:
        raise ValueError(
            "Document content is not downloaded yet. "
            "Sync with download_content=true first."
        )
    if response_config_id is None:
        response_config_id = await _pick_default_supplier_response_config_id(
            session,
            provider_id=target_provider_id,
        )

    if document.supplier_order_message_id is not None:
        existing = await session.get(
            SupplierOrderMessage,
            document.supplier_order_message_id,
        )
        if existing is not None:
            return existing

    file_path = str(document.local_file_path)
    if not os.path.exists(os.path.abspath(file_path)):
        raise ValueError("Local document file is missing on disk")

    message_row = SupplierOrderMessage(
        supplier_order_id=None,
        provider_id=target_provider_id,
        message_type="SHIPPING_DOC",
        subject=(
            document.document_number
            or document.file_name
            or "Документ из Диадок"
        ),
        sender_email=document.counteragent_box_id or "diadoc",
        received_at=document.delivery_at or document.sent_at or now_moscow(),
        body_preview="Импортировано из Диадок",
        raw_status=None,
        normalized_status=None,
        parse_confidence=None,
        source_uid=None,
        source_message_id=None,
        response_config_id=response_config_id,
        import_error_details=(
            f"Импортировано из Диадок: {document.environment}/"
            f"{document.box_id_guid}/{document.message_id}/{document.entity_id}"
        )[:500],
        mapping_id=None,
    )
    session.add(message_row)
    await session.flush()

    attachment = SupplierOrderAttachment(
        message_id=message_row.id,
        filename=document.file_name or os.path.basename(file_path),
        mime_type=None,
        file_path=file_path,
        sha256=document.content_sha256,
        parsed_kind="diadoc_document",
    )
    session.add(attachment)
    document.provider_id = target_provider_id
    document.supplier_order_message_id = message_row.id
    document.status = "registered"
    document.registered_at = now_moscow()
    document.import_error_details = None
    session.add(document)
    await session.commit()
    await session.refresh(message_row)
    return message_row


async def process_diadoc_incoming_document(
    session: AsyncSession,
    *,
    document: DiadocIncomingDocument,
    provider_id: int | None = None,
    response_config_id: int | None = None,
    download_content_if_missing: bool = True,
    register_if_needed: bool = True,
    client: DiadocClient | None = None,
) -> dict[str, Any]:
    target_provider_id = int(provider_id or document.provider_id or 0)
    if target_provider_id <= 0:
        raise ValueError(
            "Provider is not resolved. "
            "Bind provider via external reference or pass provider_id."
        )

    if download_content_if_missing and not document.local_file_path:
        if client is None:
            raise ValueError(
                "Document content is missing and no Diadoc client was provided"
            )
        document = await ensure_diadoc_document_content(
            session,
            client=client,
            document=document,
        )

    message_row: SupplierOrderMessage | None = None
    if document.supplier_order_message_id is not None:
        message_row = await session.get(
            SupplierOrderMessage,
            document.supplier_order_message_id,
        )
    if message_row is None:
        if not register_if_needed:
            raise ValueError(
                "Document is not registered as a supplier message yet"
            )
        message_row = await register_diadoc_document_as_supplier_message(
            session,
            document=document,
            provider_id=target_provider_id,
            response_config_id=response_config_id,
        )
        document = await get_diadoc_incoming_document(
            session,
            document_id=int(document.id),
        )
        if document is None:
            raise LookupError("Document not found after registration")
    else:
        target_provider_id = int(message_row.provider_id)

    resolved_config_id = int(
        response_config_id or message_row.response_config_id or 0
    )
    if resolved_config_id <= 0:
        auto_config_id = await _pick_default_supplier_response_config_id(
            session,
            provider_id=target_provider_id,
        )
        if auto_config_id is None:
            raise ValueError(
                "Supplier response config is not resolved. "
                "Pass response_config_id explicitly."
            )
        resolved_config_id = int(auto_config_id)
        if message_row.response_config_id != resolved_config_id:
            message_row.response_config_id = resolved_config_id
            session.add(message_row)
            await session.commit()

    result = await process_stored_supplier_document_message(
        session,
        provider_id=target_provider_id,
        supplier_response_config_id=resolved_config_id,
        message_id=int(message_row.id),
        date_from=document.document_date,
    )
    document.provider_id = target_provider_id
    document.supplier_order_message_id = int(message_row.id)
    document.import_error_details = result.get("import_error_details")
    if result.get("message_type") == "IMPORT_ERROR":
        document.status = "error"
    elif result.get("already_processed") or result.get("receipt_ids"):
        document.status = "processed"
    else:
        document.status = "registered"
    session.add(document)
    await session.commit()
    return result


async def sync_diadoc_incoming_documents(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    filter_category: str,
    count: int,
    after_index_key: str | None = None,
    counteragent_box_id: str | None = None,
    document_number: str | None = None,
    from_document_date=None,
    to_document_date=None,
    sort_direction: str = "Descending",
    download_content: bool = True,
    register_supplier_message: bool = False,
    process_supplier_message: bool = False,
) -> dict[str, Any]:
    payload = await client.get_documents(
        box_id_guid=box_id_guid,
        filter_category=filter_category,
        count=count,
        after_index_key=after_index_key,
        counteragent_box_id=counteragent_box_id,
        document_number=document_number,
        from_document_date=from_document_date,
        to_document_date=to_document_date,
        sort_direction=sort_direction,
    )
    result = {
        "total_from_api": int(payload.get("TotalCount") or 0),
        "synced": 0,
        "created": 0,
        "updated": 0,
        "downloaded": 0,
        "registered_supplier_messages": 0,
        "processed_supplier_messages": 0,
        "processing_skipped": 0,
        "provider_resolved": 0,
        "provider_unresolved": 0,
        "errors": [],
    }
    for raw in payload.get("Documents") or []:
        message_id = str(raw.get("MessageId") or "").strip()
        entity_id = str(raw.get("EntityId") or "").strip()
        if not message_id or not entity_id:
            result["errors"].append(
                "Skipped document without message/entity id"
            )
            continue
        stmt = select(DiadocIncomingDocument).where(
            DiadocIncomingDocument.environment == environment,
            DiadocIncomingDocument.box_id_guid == box_id_guid,
            DiadocIncomingDocument.message_id == message_id,
            DiadocIncomingDocument.entity_id == entity_id,
        )
        document = (await session.execute(stmt)).scalar_one_or_none()
        if document is None:
            document = DiadocIncomingDocument(
                environment=environment,
                box_id_guid=box_id_guid,
                message_id=message_id,
                entity_id=entity_id,
            )
            session.add(document)
            result["created"] += 1
            existing_status = None
        else:
            result["updated"] += 1
            existing_status = (
                str(document.status or "").strip().lower() or None
            )

        resolved_provider_id = (
            await resolve_provider_by_diadoc_counteragent_box(
                session,
                counteragent_box_id=raw.get("CounteragentBoxId"),
            )
        )
        if resolved_provider_id is not None:
            result["provider_resolved"] += 1
        else:
            result["provider_unresolved"] += 1

        document.index_key = raw.get("IndexKey")
        document.counteragent_box_id = raw.get("CounteragentBoxId")
        document.file_name = raw.get("FileName")
        document.document_number = raw.get("DocumentNumber")
        document.document_date = _parse_document_date(raw.get("DocumentDate"))
        document.delivery_timestamp_ticks = raw.get("DeliveryTimestampTicks")
        document.send_timestamp_ticks = raw.get("SendTimestampTicks")
        document.delivery_at = _ticks_to_datetime(
            raw.get("DeliveryTimestampTicks")
        )
        document.sent_at = _ticks_to_datetime(raw.get("SendTimestampTicks"))
        document.provider_id = resolved_provider_id
        document.raw_metadata = raw
        if existing_status in (None, "", "synced"):
            document.status = "synced"
        document.synced_at = now_moscow()
        session.add(document)
        await session.flush()
        result["synced"] += 1

        try:
            if download_content or process_supplier_message:
                before_path = document.local_file_path
                await ensure_diadoc_document_content(
                    session,
                    client=client,
                    document=document,
                )
                if not before_path and document.local_file_path:
                    result["downloaded"] += 1
            if (
                (register_supplier_message or process_supplier_message)
                and document.provider_id is not None
                and document.supplier_order_message_id is None
            ):
                await register_diadoc_document_as_supplier_message(
                    session,
                    document=document,
                )
                result["registered_supplier_messages"] += 1
            if process_supplier_message:
                if document.provider_id is None:
                    result["processing_skipped"] += 1
                else:
                    await process_diadoc_incoming_document(
                        session,
                        document=document,
                        provider_id=int(document.provider_id),
                        download_content_if_missing=False,
                        register_if_needed=True,
                    )
                    result["processed_supplier_messages"] += 1
        except Exception as exc:
            document.status = "error"
            document.import_error_details = str(exc)[:2000]
            session.add(document)
            await session.commit()
            result["errors"].append(f"{message_id}/{entity_id}: {exc}")
    return result
