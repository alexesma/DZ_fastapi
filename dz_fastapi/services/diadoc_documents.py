import hashlib
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import aiofiles
from sqlalchemy import func, select
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
from dz_fastapi.services.diadoc_boxes import diadoc_box_variants
from dz_fastapi.services.diadoc_corrections import fill_correction_from_document
from dz_fastapi.services.diadoc_direction import detect_document_direction
from dz_fastapi.services.supplier_document_dedup import (
    apply_category_status,
    apply_diadoc_metadata_identity,
    evaluate_diadoc_document,
)
from dz_fastapi.services.supplier_documents import register_supplier_document_from_diadoc
from dz_fastapi.services.supplier_order_responses import process_stored_supplier_document_message
from dz_fastapi.services.supplier_receipt_corrections import (
    get_correction,
    is_correction_document,
    register_incoming_correction,
)

DIADOC_DOCUMENTS_DIR = os.getenv(
    "DIADOC_DOCUMENTS_DIR",
    "uploads/diadoc_documents",
)
DIADOC_PROVIDER_SOURCE_SYSTEMS = (
    "DIADOC_BOX",
    "DIADOC_COUNTERAGENT_BOX",
)
logger = logging.getLogger("dz_fastapi")
_REVIEW_ONLY_PREFIX = "Строки документа не полностью сопоставлены"
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
    entity_id: str,
    filename: str | None,
) -> str:
    safe_box = _safe_filename(box_id_guid)
    safe_message = _safe_filename(message_id)
    safe_entity = _safe_filename(entity_id)
    date_str = now_moscow().strftime("%Y%m%d")
    # Имя файла в ФС не длиннее 255 байт: сокращаем исходное имя,
    # сохраняя расширение.
    raw_name = _INVALID_FILENAME_CHARS_RE.sub("_", str(filename or "").strip())
    stem, ext = os.path.splitext(raw_name)
    ext = ext[:10]
    max_stem_len = max(10, 200 - len(safe_message) - len(safe_entity) - 2 - len(ext))
    safe_name = (stem[:max_stem_len] + ext) or "document.bin"
    return os.path.join(
        DIADOC_DOCUMENTS_DIR,
        environment,
        safe_box,
        date_str,
        f"{safe_message}_{safe_entity}_{safe_name}",
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
            selectinload(DiadocIncomingDocument.supplier_order_message).selectinload(
                SupplierOrderMessage.receipts
            ),
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
            selectinload(DiadocIncomingDocument.supplier_order_message).selectinload(
                SupplierOrderMessage.receipts
            ),
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
        stmt = stmt.where(DiadocIncomingDocument.supplier_order_message_id.is_not(None))
    elif registered_only is False:
        stmt = stmt.where(DiadocIncomingDocument.supplier_order_message_id.is_(None))
    return list((await session.execute(stmt)).scalars().all())


async def resolve_provider_by_diadoc_counteragent_box(
    session: AsyncSession,
    *,
    counteragent_box_id: str | None,
) -> int | None:
    variants = diadoc_box_variants(counteragent_box_id)
    if not variants:
        return None
    stmt = (
        select(ProviderExternalReference)
        .where(
            ProviderExternalReference.is_active.is_(True),
            ProviderExternalReference.source_system.in_(DIADOC_PROVIDER_SOURCE_SYSTEMS),
            func.lower(ProviderExternalReference.external_supplier_name).in_(variants),
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
    if document.local_file_path and os.path.exists(os.path.abspath(document.local_file_path)):
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
        entity_id=document.entity_id,
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
    file_name: str | None = None,
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
    # Несколько конфигураций (например, для писем и для Диадока):
    # выбираем ту, чей шаблон имени файла документа подходит.
    if file_name:
        matching = []
        for config in configs:
            pattern = (config.shipping_doc_filename_pattern or "").strip()
            if not pattern:
                continue
            try:
                if re.search(pattern, file_name):
                    matching.append(config)
            except re.error:
                continue
        if len(matching) == 1:
            return int(matching[0].id)
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
            "Provider is not resolved. " "Bind provider via external reference or pass provider_id."
        )
    if document.direction == "customer":
        raise ValueError(
            "Документ по нашей продаже клиенту: как поставщиковый " "не регистрируется"
        )
    if not document.local_file_path:
        raise ValueError(
            "Document content is not downloaded yet. " "Sync with download_content=true first."
        )
    if response_config_id is None:
        response_config_id = await _pick_default_supplier_response_config_id(
            session,
            provider_id=target_provider_id,
            file_name=document.file_name,
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
        message_type=(
            "CORRECTION_DOC"
            if is_correction_document(
                (document.raw_metadata or {}).get("TypeNamedId"),
                document.file_name,
            )
            else "SHIPPING_DOC"
        ),
        subject=(document.document_number or document.file_name or "Документ из Диадок"),
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
    if message_row.message_type == "CORRECTION_DOC":
        await register_incoming_correction(
            session,
            provider_id=target_provider_id,
            message_id=int(message_row.id),
            source_kind="diadoc",
            document_number=document.document_number,
            document_date=document.document_date,
        )
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
            "Provider is not resolved. " "Bind provider via external reference or pass provider_id."
        )

    if download_content_if_missing and not document.local_file_path:
        if client is None:
            raise ValueError("Document content is missing and no Diadoc client was provided")
        document = await ensure_diadoc_document_content(
            session,
            client=client,
            document=document,
        )

    verdict = await evaluate_diadoc_document(session, document=document)
    if verdict["action"] != "proceed":
        return {
            "message_id": int(verdict.get("message_id") or document.supplier_order_message_id or 0),
            "provider_id": target_provider_id,
            "receipt_ids": [],
            "dedup_action": verdict["action"],
            "import_error_details": document.import_error_details,
            "already_processed": verdict["action"] == "linked",
        }

    message_row: SupplierOrderMessage | None = None
    if document.supplier_order_message_id is not None:
        message_row = await session.get(
            SupplierOrderMessage,
            document.supplier_order_message_id,
        )
    if message_row is None:
        if not register_if_needed:
            raise ValueError("Document is not registered as a supplier message yet")
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

    if message_row.message_type == "CORRECTION_DOC" or is_correction_document(
        (document.raw_metadata or {}).get("TypeNamedId"),
        document.file_name,
    ):
        message_row.message_type = "CORRECTION_DOC"
        correction = await register_incoming_correction(
            session,
            provider_id=target_provider_id,
            message_id=int(message_row.id),
            source_kind="diadoc",
            document_number=document.document_number,
            document_date=document.document_date,
        )
        await session.flush()
        correction = await get_correction(session, int(correction.id))
        if correction.status == "draft" and not correction.items:
            summary = await fill_correction_from_document(
                session, correction=correction, document=document
            )
            document.parsed_details = summary
            document.import_error_details = summary.get("hint")
        else:
            document.import_error_details = None
        document.status = "registered"
        await session.commit()
        return {
            "message_id": int(message_row.id),
            "provider_id": target_provider_id,
            "message_type": "CORRECTION_DOC",
            "correction_id": int(correction.id),
            "receipt_ids": [],
            "needs_review": True,
        }

    resolved_config_id = int(response_config_id or message_row.response_config_id or 0)
    if resolved_config_id <= 0:
        auto_config_id = await _pick_default_supplier_response_config_id(
            session,
            provider_id=target_provider_id,
            file_name=document.file_name,
        )
        if auto_config_id is None:
            raise ValueError(
                "Supplier response config is not resolved. " "Pass response_config_id explicitly."
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
    )
    document.provider_id = target_provider_id
    document.supplier_order_message_id = int(message_row.id)
    document.import_error_details = result.get("import_error_details")
    details = str(result.get("import_error_details") or "").strip()
    review_only = bool(result.get("receipt_ids")) and details.startswith(
        _REVIEW_ONLY_PREFIX
    ) and ";" not in details
    if result.get("message_type") == "IMPORT_ERROR" and review_only:
        # Черновик создан, строки нужно лишь проверить — это не сбой.
        document.status = "registered"
    elif result.get("message_type") == "IMPORT_ERROR":
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
    process_not_before: date | None = None,
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
    documents_payload = list(payload.get("Documents") or [])
    result = {
        "total_from_api": int(payload.get("TotalCount") or 0),
        "has_more_results": bool(payload.get("HasMoreResults")),
        "last_index_key": (
            str(documents_payload[-1].get("IndexKey") or "") or None if documents_payload else None
        ),
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
    for raw in documents_payload:
        message_id = str(raw.get("MessageId") or "").strip()
        entity_id = str(raw.get("EntityId") or "").strip()
        if not message_id or not entity_id:
            result["errors"].append("Skipped document without message/entity id")
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
            existing_status = str(document.status or "").strip().lower() or None

        resolved_provider_id = await resolve_provider_by_diadoc_counteragent_box(
            session,
            counteragent_box_id=raw.get("CounteragentBoxId"),
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
        document.delivery_at = _ticks_to_datetime(raw.get("DeliveryTimestampTicks"))
        document.sent_at = _ticks_to_datetime(raw.get("SendTimestampTicks"))
        document.provider_id = resolved_provider_id
        document.raw_metadata = raw
        apply_diadoc_metadata_identity(document)
        apply_category_status(document)
        if existing_status in (None, "", "synced"):
            document.status = "synced"
        document.synced_at = now_moscow()
        session.add(document)
        await session.flush()
        result["synced"] += 1

        try:
            async with session.begin_nested():
                await register_supplier_document_from_diadoc(
                    session, document
                )
        except Exception:  # реестр не должен ломать загрузку документов
            logger.exception("Supplier document registry failed")
        too_old = process_not_before is not None and (
            document.document_date is None
            or document.document_date < process_not_before
        )
        do_register = register_supplier_message and not too_old
        do_process = process_supplier_message and not too_old
        try:
            if download_content or do_process:
                before_path = document.local_file_path
                await ensure_diadoc_document_content(
                    session,
                    client=client,
                    document=document,
                )
                if not before_path and document.local_file_path:
                    result["downloaded"] += 1
            if document.local_file_path and document.direction in (
                None,
                "unknown",
            ):
                await detect_document_direction(session, document)
            if (
                (do_register or do_process)
                and document.provider_id is not None
                and document.supplier_order_message_id is None
            ):
                verdict = await evaluate_diadoc_document(session, document=document)
                if verdict["action"] != "proceed":
                    result["dedup_" + verdict["action"]] = (
                        result.get("dedup_" + verdict["action"], 0) + 1
                    )
                    session.add(document)
                    await session.commit()
                    continue
                await register_diadoc_document_as_supplier_message(
                    session,
                    document=document,
                )
                result["registered_supplier_messages"] += 1
            if do_process:
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


async def sync_diadoc_incoming_document_by_id(
    session: AsyncSession,
    *,
    client: DiadocClient,
    environment: str,
    box_id_guid: str,
    message_id: str,
    entity_id: str,
    event_index_key: str | None = None,
    download_content: bool = True,
    register_supplier_message: bool = False,
    process_supplier_message: bool = False,
) -> dict[str, Any]:
    """Загружает конкретный документ, найденный в ленте BoxEvent.

    В отличие от GetDocuments здесь нет повторного поиска и пагинации: пара
    MessageId/EntityId берётся прямо из события.
    """
    raw = await client.get_document(
        box_id_guid=box_id_guid,
        message_id=message_id,
        entity_id=entity_id,
    )
    stmt = select(DiadocIncomingDocument).where(
        DiadocIncomingDocument.environment == environment,
        DiadocIncomingDocument.box_id_guid == box_id_guid,
        DiadocIncomingDocument.message_id == message_id,
        DiadocIncomingDocument.entity_id == entity_id,
    )
    document = (await session.execute(stmt)).scalar_one_or_none()
    created = document is None
    if document is None:
        document = DiadocIncomingDocument(
            environment=environment,
            box_id_guid=box_id_guid,
            message_id=message_id,
            entity_id=entity_id,
        )
        session.add(document)
        existing_status = None
    else:
        existing_status = str(document.status or "").strip().lower() or None

    provider_id = await resolve_provider_by_diadoc_counteragent_box(
        session,
        counteragent_box_id=raw.get("CounteragentBoxId"),
    )
    document.index_key = event_index_key or raw.get("IndexKey")
    document.counteragent_box_id = raw.get("CounteragentBoxId")
    document.file_name = raw.get("FileName")
    document.document_number = raw.get("DocumentNumber")
    document.document_date = _parse_document_date(raw.get("DocumentDate"))
    document.delivery_timestamp_ticks = raw.get("DeliveryTimestampTicks")
    document.send_timestamp_ticks = raw.get("SendTimestampTicks")
    document.delivery_at = _ticks_to_datetime(raw.get("DeliveryTimestampTicks"))
    document.sent_at = _ticks_to_datetime(raw.get("SendTimestampTicks"))
    document.provider_id = provider_id
    document.raw_metadata = raw
    apply_diadoc_metadata_identity(document)
    apply_category_status(document)
    if existing_status in (None, "", "synced"):
        document.status = "synced"
    document.synced_at = now_moscow()
    session.add(document)
    await session.flush()

    downloaded = False
    registered = False
    processed = False
    try:
        if download_content or process_supplier_message:
            before_path = document.local_file_path
            await ensure_diadoc_document_content(session, client=client, document=document)
            downloaded = not before_path and bool(document.local_file_path)
        if document.local_file_path and document.direction in (None, "unknown"):
            await detect_document_direction(session, document)
        if (
            (register_supplier_message or process_supplier_message)
            and document.provider_id is not None
            and document.supplier_order_message_id is None
        ):
            verdict = await evaluate_diadoc_document(session, document=document)
            if verdict["action"] == "proceed":
                await register_diadoc_document_as_supplier_message(session, document=document)
                registered = True
        if process_supplier_message and document.provider_id is not None:
            await process_diadoc_incoming_document(
                session,
                document=document,
                provider_id=int(document.provider_id),
                download_content_if_missing=False,
                register_if_needed=True,
            )
            processed = True
    except Exception as exc:
        document.status = "error"
        document.import_error_details = str(exc)[:2000]
        session.add(document)
        await session.flush()
        raise
    return {
        "document_id": document.id,
        "created": created,
        "downloaded": downloaded,
        "registered_supplier_message": registered,
        "processed_supplier_message": processed,
        "provider_resolved": provider_id is not None,
    }
