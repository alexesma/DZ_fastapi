"""Облачная подпись Диадока (Контур.Сертификат): двухшаговый флоу с SMS.

Шаг 1 (start): готовим файлы (титул покупателя + ИоП, запрос на
аннулирование или контент исходящего), отдаём в CloudSign — пользователю
приходит SMS. Шаг 2 (confirm): SMS-код → подписи → PostMessagePatch или
PostMessage. Файлы между шагами храним в задаче (base64).
"""

from __future__ import annotations

import base64
import hashlib
import logging
import zlib
from typing import Any, Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.models.diadoc import (
    DiadocCloudSignTask,
    DiadocIncomingDocument,
    DiadocOutgoingDocument,
)
from dz_fastapi.models.settings import DiadocIntegrationSettings

logger = logging.getLogger("dz_fastapi")

OPERATION_SIGN_INCOMING = "sign_incoming"
OPERATION_REVOKE_OUTGOING = "revoke_outgoing"
OPERATION_SEND_OUTGOING = "send_outgoing"
OPERATION_REJECT_INCOMING = "reject_incoming"

# Версия УПД по умолчанию: формат 5.03 обязателен с 01.04.2025.
DEFAULT_UTD_VERSION = "utd970_05_03_01"
DEFAULT_UTD_FUNCTION = "СЧФДОП"
DEFAULT_UCD_VERSION = "ucd29_05_02_01"
DEFAULT_UCD_FUNCTION = "КСЧФДИС"


async def _active_task(
    session: AsyncSession,
    *,
    operation: str,
    incoming_document_id: int | None = None,
    outgoing_document_id: int | None = None,
) -> DiadocCloudSignTask | None:
    document_id = incoming_document_id or outgoing_document_id
    if document_id is not None and session.get_bind().dialect.name == "postgresql":
        lock_key = zlib.crc32(f"cloud-sign:{operation}".encode("utf-8"))
        if lock_key >= 2**31:
            lock_key -= 2**32
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:operation_key, :document_id)"),
            {"operation_key": lock_key, "document_id": int(document_id)},
        )
    conditions = [
        DiadocCloudSignTask.operation == operation,
        DiadocCloudSignTask.state == "waiting_code",
    ]
    if incoming_document_id is not None:
        conditions.append(DiadocCloudSignTask.incoming_document_id == incoming_document_id)
    if outgoing_document_id is not None:
        conditions.append(DiadocCloudSignTask.outgoing_document_id == outgoing_document_id)
    return await session.scalar(
        select(DiadocCloudSignTask)
        .where(*conditions)
        .order_by(DiadocCloudSignTask.id.desc())
        .limit(1)
    )


def split_signer_full_name(
    full_name: Optional[str],
) -> tuple[str, str, Optional[str]]:
    """«Фамилия Имя Отчество» → (фамилия, имя, отчество|None)."""
    parts = [part for part in str(full_name or "").strip().split() if part]
    if len(parts) < 2:
        raise ValueError(
            "В профиле Диадока не заполнено ФИО подписанта " "(нужно минимум «Фамилия Имя»)"
        )
    surname = parts[0]
    first_name = parts[1]
    patronymic = " ".join(parts[2:]) or None
    return surname, first_name, patronymic


def build_signer_payload(
    integration: DiadocIntegrationSettings,
) -> dict[str, Any]:
    """Signer для GenerateReceiptXml/GenerateRevocationRequestXml."""
    surname, first_name, patronymic = split_signer_full_name(integration.signer_full_name)
    details: dict[str, Any] = {
        "Surname": surname,
        "FirstName": first_name,
    }
    if patronymic:
        details["Patronymic"] = patronymic
    position = str(integration.signer_position or "").strip()
    if position:
        details["JobTitle"] = position
    inn = str(integration.organization_inn or "").strip()
    if inn:
        details["Inn"] = inn
    return {"SignerDetails": details}


def _xml_escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_utd970_buyer_title_user_data(
    integration: DiadocIntegrationSettings,
    *,
    acceptance_date: str,
    total_code: str = "1",
) -> bytes:
    """User-data XML титула покупателя УПД (формат 970/5.03).

    total_code: 1 — принято без расхождений, 2 — с расхождениями,
    3 — не принято.
    """
    surname, first_name, patronymic = split_signer_full_name(integration.signer_full_name)
    position = str(integration.signer_position or "").strip()
    if not position:
        raise ValueError("В профиле Диадока не заполнена должность подписанта")
    organization_name = str(integration.organization_name or "").strip()
    fio_attrs = f'LastName="{_xml_escape(surname)}" ' f'FirstName="{_xml_escape(first_name)}"'
    if patronymic:
        fio_attrs += f' MiddleName="{_xml_escape(patronymic)}"'
    creator_attr = (
        f' DocumentCreator="{_xml_escape(organization_name)}"' if organization_name else ""
    )
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<UniversalTransferDocumentBuyerTitle "
        f'AcceptanceDate="{_xml_escape(acceptance_date)}"'
        f"{creator_attr}>\n"
        f'  <ContentOperCode TotalCode="{_xml_escape(total_code)}" />\n'
        "  <Signers>\n"
        '    <Signer SignerPowersConfirmationMethod="1">\n'
        f"      <Fio {fio_attrs} />\n"
        '      <Position PositionSource="Manual">'
        f"{_xml_escape(position)}</Position>\n"
        "    </Signer>\n"
        "  </Signers>\n"
        "</UniversalTransferDocumentBuyerTitle>\n"
    )
    return xml.encode("utf-8")


def build_ucd_buyer_title_user_data(
    integration: DiadocIntegrationSettings,
    *,
    approval_date: str,
    operation_content: str = "1",
) -> bytes:
    """UserDataXml титула покупателя УКД текущего формата приказа №29."""
    surname, first_name, patronymic = split_signer_full_name(integration.signer_full_name)
    position = str(integration.signer_position or "").strip()
    if not position:
        raise ValueError("В профиле Диадока не заполнена должность подписанта")
    fio_attrs = f'LastName="{_xml_escape(surname)}" ' f'FirstName="{_xml_escape(first_name)}"'
    if patronymic:
        fio_attrs += f' MiddleName="{_xml_escape(patronymic)}"'
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<UniversalCorrectionDocumentBuyerTitle "
        f'OperationContent="{_xml_escape(operation_content)}" '
        f'ApprovalDate="{_xml_escape(approval_date)}" '
        'xmlns:xs="http://www.w3.org/2001/XMLSchema">\n'
        "  <Signers>\n"
        '    <Signer SignerPowersConfirmationMethod="1">\n'
        f"      <Fio {fio_attrs} />\n"
        '      <Position PositionSource="Manual">'
        f"{_xml_escape(position)}</Position>\n"
        "    </Signer>\n"
        "  </Signers>\n"
        "</UniversalCorrectionDocumentBuyerTitle>\n"
    )
    return xml.encode("utf-8")


def _find_first_string(
    payload: Any,
    keys: tuple[str, ...],
    *,
    max_depth: int = 6,
) -> Optional[str]:
    """Рекурсивно ищет первое непустое строковое значение по ключам."""
    if max_depth < 0:
        return None
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in payload.values():
            found = _find_first_string(value, keys, max_depth=max_depth - 1)
            if found:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _find_first_string(item, keys, max_depth=max_depth - 1)
            if found:
                return found
    return None


async def _resolve_incoming_function_and_version(
    client: DiadocClient,
    document: DiadocIncomingDocument,
) -> tuple[str, str]:
    """Function/Version входящего УПД — нужны для генерации титула."""
    metadata = document.raw_metadata or {}
    function = _find_first_string(metadata, ("Function", "DocumentFunction"))
    version = _find_first_string(metadata, ("Version", "DocumentVersion"))
    if not function or not version:
        try:
            payload = await client.get_document(
                box_id_guid=document.box_id_guid,
                message_id=document.message_id,
                entity_id=document.entity_id,
            )
            function = function or _find_first_string(payload, ("Function", "DocumentFunction"))
            version = version or _find_first_string(payload, ("Version", "DocumentVersion"))
        except Exception:  # noqa: BLE001
            logger.warning(
                "Failed to resolve function/version for incoming "
                "Diadoc document #%s, falling back to defaults",
                document.id,
            )
    from dz_fastapi.services.supplier_document_dedup import effective_category

    if effective_category(document) == "ukd":
        return function or DEFAULT_UCD_FUNCTION, version or DEFAULT_UCD_VERSION
    return function or DEFAULT_UTD_FUNCTION, version or DEFAULT_UTD_VERSION


def _encode_file(content: bytes) -> str:
    return base64.b64encode(content).decode("ascii")


def _task_operation_id(task: DiadocCloudSignTask, action: str) -> str:
    """Стабильный ID защищает повтор SMS-подтверждения от дубля в Диадоке."""
    source = (
        f"{action}|{task.environment}|{task.box_id_guid}|"
        f"{task.message_id or ''}|{task.entity_id or ''}|{task.id or ''}"
    )
    return hashlib.md5(source.encode("utf-8")).hexdigest()


async def start_incoming_sign_task(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    document: DiadocIncomingDocument,
    include_receipt: bool = True,
    total_code: str = "1",
    audit: dict[str, Any] | None = None,
) -> DiadocCloudSignTask:
    """Готовит титул покупателя (+ИоП) и стартует облачное подписание.

    ``audit`` — кто запустил подпись и что показала сверка с приёмкой:
    сохраняется в параметрах задачи для разбора спорных случаев.
    """
    if not document.message_id or not document.entity_id:
        raise ValueError("У документа нет message_id/entity_id")
    existing = await _active_task(
        session,
        operation=OPERATION_SIGN_INCOMING,
        incoming_document_id=int(document.id),
    )
    if existing is not None:
        return existing
    function, version = await _resolve_incoming_function_and_version(client, document)
    from dz_fastapi.services.supplier_document_dedup import effective_category

    category = effective_category(document)
    acceptance_date = now_moscow().strftime("%d.%m.%Y")
    if category == "ukd":
        type_named_id = "UniversalCorrectionDocument"
        user_data = build_ucd_buyer_title_user_data(
            integration,
            approval_date=acceptance_date,
            operation_content="1",
        )
    else:
        type_named_id = "UniversalTransferDocument"
        user_data = build_utd970_buyer_title_user_data(
            integration,
            acceptance_date=acceptance_date,
            total_code=total_code,
        )
    title_content, title_file_name = await client.generate_title_xml(
        box_id_guid=document.box_id_guid,
        document_type_named_id=type_named_id,
        document_function=function,
        document_version=version,
        title_index=1,
        user_data_xml=user_data,
        letter_id=document.message_id,
        document_id=document.entity_id,
    )
    files: list[dict[str, Any]] = [
        {
            "file_name": title_file_name or "buyer_title.xml",
            "content_b64": _encode_file(title_content),
            "kind": "buyer_title",
            "parent_entity_id": document.entity_id,
        }
    ]
    if include_receipt:
        receipt_content = await client.generate_receipt_xml(
            box_id_guid=document.box_id_guid,
            message_id=document.message_id,
            attachment_id=document.entity_id,
            signer=build_signer_payload(integration),
        )
        files.append(
            {
                "file_name": "receipt.xml",
                "content_b64": _encode_file(receipt_content),
                "kind": "receipt",
                "parent_entity_id": document.entity_id,
            }
        )
    token = await client.cloud_sign(
        files=[
            {
                "FileName": item["file_name"],
                "Content": {"Content": item["content_b64"]},
            }
            for item in files
        ]
    )
    task = DiadocCloudSignTask(
        environment=document.environment,
        operation=OPERATION_SIGN_INCOMING,
        state="waiting_code",
        box_id_guid=document.box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
        incoming_document_id=int(document.id),
        cloud_sign_token=token,
        files=files,
        params={
            "function": function,
            "version": version,
            "total_code": total_code,
            "type_named_id": type_named_id,
            **(audit or {}),
        },
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def start_reject_incoming_task(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    document: DiadocIncomingDocument,
    comment: str,
) -> DiadocCloudSignTask:
    """Отказ в подписи входящего: XML отказа → CloudSign → SMS."""
    if not document.message_id or not document.entity_id:
        raise ValueError("У документа нет message_id/entity_id")
    existing = await _active_task(
        session,
        operation=OPERATION_REJECT_INCOMING,
        incoming_document_id=int(document.id),
    )
    if existing is not None:
        return existing
    reason = str(comment or "").strip()
    if not reason:
        raise ValueError("Укажите причину отказа в подписи")
    rejection_content = await client.generate_signature_rejection_xml(
        box_id_guid=document.box_id_guid,
        message_id=document.message_id,
        attachment_id=document.entity_id,
        comment=reason,
        signer=build_signer_payload(integration),
    )
    files = [
        {
            "file_name": "signature_rejection.xml",
            "content_b64": _encode_file(rejection_content),
            "kind": "signature_rejection",
            "parent_entity_id": document.entity_id,
        }
    ]
    token = await client.cloud_sign(
        files=[
            {
                "FileName": files[0]["file_name"],
                "Content": {"Content": files[0]["content_b64"]},
            }
        ]
    )
    task = DiadocCloudSignTask(
        environment=document.environment,
        operation=OPERATION_REJECT_INCOMING,
        state="waiting_code",
        box_id_guid=document.box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
        incoming_document_id=int(document.id),
        cloud_sign_token=token,
        files=files,
        params={"comment": reason},
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def _apply_reject_incoming(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    files = list(task.files or [])
    patch = {
        "BoxId": task.box_id_guid,
        "MessageId": task.message_id,
        "XmlSignatureRejections": [
            {
                "ParentEntityId": files[0]["parent_entity_id"],
                "SignedContent": _signed_content(files[0], signatures[0]),
            }
        ],
    }
    response = await client.post_message_patch(
        patch=patch,
        operation_id=_task_operation_id(task, "reject-incoming"),
    )
    if task.incoming_document_id:
        document = await session.get(DiadocIncomingDocument, task.incoming_document_id)
        if document is not None:
            document.rejected_at = now_moscow()
            session.add(document)
    return {"patch_response_keys": sorted(response.keys())}


async def start_revoke_outgoing_task(
    session: AsyncSession,
    *,
    client: DiadocClient,
    integration: DiadocIntegrationSettings,
    document: DiadocOutgoingDocument,
    comment: Optional[str] = None,
) -> DiadocCloudSignTask:
    """Готовит запрос на аннулирование и стартует облачное подписание."""
    if not document.message_id or not document.entity_id:
        raise ValueError("У документа нет message_id/entity_id")
    existing = await _active_task(
        session,
        operation=OPERATION_REVOKE_OUTGOING,
        outgoing_document_id=int(document.id),
    )
    if existing is not None:
        return existing
    revocation_content = await client.generate_revocation_request_xml(
        box_id_guid=document.from_box_id_guid,
        message_id=document.message_id,
        attachment_id=document.entity_id,
        comment=comment,
        signer=build_signer_payload(integration),
    )
    files = [
        {
            "file_name": "revocation_request.xml",
            "content_b64": _encode_file(revocation_content),
            "kind": "revocation_request",
            "parent_entity_id": document.entity_id,
        }
    ]
    token = await client.cloud_sign(
        files=[
            {
                "FileName": files[0]["file_name"],
                "Content": {"Content": files[0]["content_b64"]},
            }
        ]
    )
    task = DiadocCloudSignTask(
        environment=document.environment,
        operation=OPERATION_REVOKE_OUTGOING,
        state="waiting_code",
        box_id_guid=document.from_box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
        outgoing_document_id=int(document.id),
        cloud_sign_token=token,
        files=files,
        params={"comment": comment or ""},
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def start_send_outgoing_task(
    session: AsyncSession,
    *,
    client: DiadocClient,
    document: DiadocOutgoingDocument,
) -> DiadocCloudSignTask:
    """Стартует подписание контента черновика для отправки."""
    import os

    if not document.is_draft:
        raise ValueError("Документ уже отправлен")
    existing = await _active_task(
        session,
        operation=OPERATION_SEND_OUTGOING,
        outgoing_document_id=int(document.id),
    )
    if existing is not None:
        return existing
    path = os.path.abspath(document.local_file_path)
    if not os.path.exists(path):
        raise ValueError("Файл документа не найден на диске — пересоздайте черновик")
    with open(path, "rb") as handle:
        content = handle.read()
    files = [
        {
            "file_name": document.file_name,
            "content_b64": _encode_file(content),
            "kind": "outgoing_content",
            "parent_entity_id": None,
        }
    ]
    token = await client.cloud_sign(
        files=[
            {
                "FileName": files[0]["file_name"],
                "Content": {"Content": files[0]["content_b64"]},
            }
        ]
    )
    task = DiadocCloudSignTask(
        environment=document.environment,
        operation=OPERATION_SEND_OUTGOING,
        state="waiting_code",
        box_id_guid=document.from_box_id_guid,
        message_id=document.message_id,
        entity_id=document.entity_id,
        outgoing_document_id=int(document.id),
        cloud_sign_token=token,
        files=files,
        params={},
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


def _signed_content(file_item: dict[str, Any], signature_b64: str) -> dict:
    return {
        "Content": file_item["content_b64"],
        "Signature": signature_b64,
    }


async def _apply_sign_incoming(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    files = list(task.files or [])
    patch: dict[str, Any] = {
        "BoxId": task.box_id_guid,
        "MessageId": task.message_id,
    }
    buyer_titles = []
    receipts = []
    for file_item, signature in zip(files, signatures):
        if file_item.get("kind") == "buyer_title":
            buyer_titles.append(
                {
                    "ParentEntityId": file_item["parent_entity_id"],
                    "NeedReceipt": False,
                    "SignedContent": _signed_content(file_item, signature),
                }
            )
        elif file_item.get("kind") == "receipt":
            receipts.append(
                {
                    "ParentEntityId": file_item["parent_entity_id"],
                    "SignedContent": _signed_content(file_item, signature),
                }
            )
    if buyer_titles:
        patch["RecipientTitles"] = buyer_titles
    if receipts:
        patch["Receipts"] = receipts
    response = await client.post_message_patch(
        patch=patch,
        operation_id=_task_operation_id(task, "sign-incoming"),
    )
    if task.incoming_document_id:
        document = await session.get(DiadocIncomingDocument, task.incoming_document_id)
        if document is not None:
            document.signed_at = now_moscow()
            session.add(document)
    return {"patch_response_keys": sorted(response.keys())}


async def _apply_revoke_outgoing(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    files = list(task.files or [])
    patch = {
        "BoxId": task.box_id_guid,
        "MessageId": task.message_id,
        "RevocationRequests": [
            {
                "ParentEntityId": files[0]["parent_entity_id"],
                "SignedContent": _signed_content(files[0], signatures[0]),
            }
        ],
    }
    response = await client.post_message_patch(
        patch=patch,
        operation_id=_task_operation_id(task, "revoke-outgoing"),
    )
    if task.outgoing_document_id:
        document = await session.get(DiadocOutgoingDocument, task.outgoing_document_id)
        if document is not None:
            document.status = "revocation_requested"
            document.docflow_status_text = "Отправлен запрос на аннулирование"
            session.add(document)
    return {"patch_response_keys": sorted(response.keys())}


async def _apply_send_outgoing(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    from dz_fastapi.services.diadoc_outgoing import _extract_first_entity_id, _extract_message_id

    document = await session.get(DiadocOutgoingDocument, task.outgoing_document_id)
    if document is None:
        raise ValueError("Исходящий документ не найден")
    operation_id = hashlib.md5(
        (
            f"send-draft|{document.environment}|{document.id}|"
            f"{document.message_id}|{document.content_sha256 or ''}"
        ).encode("utf-8")
    ).hexdigest()
    response = await client.send_draft(
        draft={
            "BoxId": document.from_box_id_guid,
            "ToBoxId": document.to_box_id_guid,
            "DraftId": document.message_id,
            "DocumentSignatures": [
                {
                    "ParentEntityId": document.entity_id,
                    "Signature": signatures[0],
                }
            ],
        },
        operation_id=operation_id,
    )
    document.message_id = _extract_message_id(response) or document.message_id
    document.entity_id = _extract_first_entity_id(response) or document.entity_id
    document.is_draft = False
    document.status = "sent"
    document.sent_at = now_moscow()
    document.raw_response = response
    session.add(document)
    return {"message_id": document.message_id}


async def confirm_cloud_sign_task(
    session: AsyncSession,
    *,
    client: DiadocClient,
    task: DiadocCloudSignTask,
    confirmation_code: str,
) -> DiadocCloudSignTask:
    """SMS-код → подписи → целевое действие по типу операции."""
    if task.state != "waiting_code":
        raise ValueError(f"Задача в состоянии {task.state} — подтверждать нечего")
    code = str(confirmation_code or "").strip()
    if not code:
        raise ValueError("Введите код подтверждения из SMS")
    try:
        signatures = await client.cloud_sign_confirm(
            token=str(task.cloud_sign_token),
            confirmation_code=code,
        )
        expected = len(list(task.files or []))
        if len(signatures) < expected:
            raise ValueError(f"Диадок вернул {len(signatures)} подписей вместо " f"{expected}")
        if task.operation == OPERATION_SIGN_INCOMING:
            details = await _apply_sign_incoming(
                session, client=client, task=task, signatures=signatures
            )
        elif task.operation == OPERATION_REVOKE_OUTGOING:
            details = await _apply_revoke_outgoing(
                session, client=client, task=task, signatures=signatures
            )
        elif task.operation == OPERATION_SEND_OUTGOING:
            details = await _apply_send_outgoing(
                session, client=client, task=task, signatures=signatures
            )
        elif task.operation == OPERATION_REJECT_INCOMING:
            details = await _apply_reject_incoming(
                session, client=client, task=task, signatures=signatures
            )
        elif task.operation == "etrn_t1_send":
            from dz_fastapi.services.etrn import apply_t1_sign

            details = await apply_t1_sign(session, client=client, task=task, signatures=signatures)
        elif task.operation == "etrn_t3_send":
            from dz_fastapi.services.etrn import apply_t3_sign

            details = await apply_t3_sign(session, client=client, task=task, signatures=signatures)
        elif task.operation == "etrn_t6_send":
            from dz_fastapi.services.etrn import apply_t6_sign

            details = await apply_t6_sign(session, client=client, task=task, signatures=signatures)
        elif task.operation == "gis_mt_auth":
            from dz_fastapi.services.gis_mt import apply_gis_mt_auth

            details = await apply_gis_mt_auth(session, task=task, signatures=signatures)
        elif task.operation == "gis_mt_withdraw":
            from dz_fastapi.services.gis_mt import apply_gis_mt_withdraw

            details = await apply_gis_mt_withdraw(session, task=task, signatures=signatures)
        else:
            raise ValueError(f"Неизвестная операция задачи: {task.operation}")
    except Exception as exc:
        task.error_details = str(exc)[:2000]
        # Код мог быть неверным — оставляем waiting_code для повторного
        # ввода; фатальные ошибки пользователь увидит в error_details.
        session.add(task)
        await session.commit()
        raise
    task.state = "completed"
    task.error_details = None
    task.completed_at = now_moscow()
    task.params = {**(task.params or {}), **details}
    task.cloud_sign_token = None
    task.files = []
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task
