"""ГИС МТ (Честный знак): вход, сверка кодов, вывод из оборота.

Все операции, требующие КЭП (вход по auth/key, подпись документа
вывода из оборота), идут через облачную подпись Диадока — тот же
двухшаговый флоу с SMS, что и подписание документов
(DiadocCloudSignTask). Токен ГИС МТ живёт ~10 часов и хранится в
GisMtSettings.
"""
from __future__ import annotations

import base64
import json
import logging
from datetime import timedelta
from typing import Any, Iterable, Optional
from uuid import uuid4

import httpx
from sqlalchemy import Text, cast, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.http.gis_mt_client import GisMtApiError, GisMtClient
from dz_fastapi.models.diadoc import DiadocCloudSignTask
from dz_fastapi.models.inventory import (
    MarkingCodeStatus,
    MarkingMovementType,
    ProductMarkingCode,
    ProductMarkingCodeMovement,
    ShipmentDocument,
    ShipmentDocumentStatus,
    ShipmentMarkingScan,
)
from dz_fastapi.models.settings import GisMtSettings

logger = logging.getLogger("dz_fastapi")

OPERATION_GIS_MT_AUTH = "gis_mt_auth"
OPERATION_GIS_MT_WITHDRAW = "gis_mt_withdraw"

GIS_MT_TOKEN_LIFETIME_HOURS = 10
GIS_MT_TOKEN_SAFETY_MINUTES = 30
GIS_CISES_INFO_CHUNK = 1000

# Причины вывода из оборота (True API). RETAIL — розничная продажа,
# OWN_USE — собственные нужды, остальные — по мере надобности.
WITHDRAWAL_ACTIONS = ("RETAIL", "OWN_USE", "EXPIRATION", "DAMAGE_LOSS")

# Статусы ГИС МТ, при которых код считается «в обороте у нас»
GIS_OK_STATUSES = {"INTRODUCED"}


async def get_or_create_gis_mt_settings(
    session: AsyncSession,
) -> GisMtSettings:
    settings_row = (
        await session.execute(select(GisMtSettings).limit(1))
    ).scalars().first()
    if settings_row is None:
        settings_row = GisMtSettings()
        session.add(settings_row)
        await session.commit()
        await session.refresh(settings_row)
    return settings_row


def gis_mt_token_is_active(settings_row: GisMtSettings) -> bool:
    if not settings_row.token or not settings_row.token_expires_at:
        return False
    return settings_row.token_expires_at > now_moscow()


def _require_active_client(settings_row: GisMtSettings) -> GisMtClient:
    if not gis_mt_token_is_active(settings_row):
        raise ValueError(
            "Токен ГИС МТ отсутствует или истёк — выполните вход "
            "(кнопка «Войти в ГИС МТ» на странице маркировки)"
        )
    return GisMtClient(token=str(settings_row.token))


async def start_gis_mt_auth_task(
    session: AsyncSession,
    *,
    diadoc_client: DiadocClient,
    environment: str,
) -> DiadocCloudSignTask:
    """Шаг 1 входа: auth/key → CloudSign(data) → SMS."""
    gis_client = GisMtClient()
    key_payload = await gis_client.get_auth_key()
    auth_uuid = str(key_payload.get("uuid") or "").strip()
    auth_data = str(key_payload.get("data") or "").strip()
    if not auth_uuid or not auth_data:
        raise ValueError("ГИС МТ не вернул uuid/data для входа")

    content_b64 = base64.b64encode(
        auth_data.encode("utf-8")
    ).decode("ascii")
    token = await diadoc_client.cloud_sign(
        files=[
            {
                "FileName": "gis_mt_auth.txt",
                "Content": {"Content": content_b64},
            }
        ]
    )
    settings_row = await get_or_create_gis_mt_settings(session)
    settings_row.auth_uuid = auth_uuid
    settings_row.last_error = None
    session.add(settings_row)

    task = DiadocCloudSignTask(
        environment=environment,
        operation=OPERATION_GIS_MT_AUTH,
        state="waiting_code",
        box_id_guid="-",
        cloud_sign_token=token,
        files=[
            {
                "file_name": "gis_mt_auth.txt",
                "content_b64": content_b64,
                "kind": "gis_auth",
                "parent_entity_id": None,
            }
        ],
        params={"uuid": auth_uuid},
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def apply_gis_mt_auth(
    session: AsyncSession,
    *,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    """Шаг 2 входа: подпись → simpleSignIn → сохранить токен."""
    auth_uuid = str((task.params or {}).get("uuid") or "").strip()
    if not auth_uuid:
        raise ValueError("В задаче нет uuid авторизации ГИС МТ")
    gis_client = GisMtClient()
    token = await gis_client.simple_sign_in(
        uuid=auth_uuid,
        signature_b64=signatures[0],
    )
    settings_row = await get_or_create_gis_mt_settings(session)
    settings_row.token = token
    settings_row.token_expires_at = now_moscow() + timedelta(
        hours=GIS_MT_TOKEN_LIFETIME_HOURS,
        minutes=-GIS_MT_TOKEN_SAFETY_MINUTES,
    )
    settings_row.last_error = None
    session.add(settings_row)
    await session.flush()
    return {
        "token_expires_at": settings_row.token_expires_at.isoformat(),
    }


def _extract_cis_and_status(item: Any) -> tuple[str, str]:
    """Достаёт (cis, status) из элемента ответа cises/info.

    Форма ответа различается между версиями API — разбираем
    защищённо: ключи на верхнем уровне или внутри cisInfo.
    """
    payload = item if isinstance(item, dict) else {}
    info = payload.get("cisInfo")
    if isinstance(info, dict):
        payload = info
    cis = str(payload.get("cis") or payload.get("requestedCis") or "")
    status = str(payload.get("status") or "")
    return cis.strip(), status.strip()


async def check_marking_codes_with_gis(
    session: AsyncSession,
    *,
    marking_code_ids: Optional[Iterable[int]] = None,
    limit: int = 1000,
) -> dict[str, Any]:
    """Сверяет наши коды со статусами в ГИС МТ."""
    settings_row = await get_or_create_gis_mt_settings(session)
    gis_client = _require_active_client(settings_row)

    stmt = select(ProductMarkingCode)
    ids = [int(value) for value in (marking_code_ids or []) if value]
    if ids:
        stmt = stmt.where(ProductMarkingCode.id.in_(ids))
    else:
        stmt = stmt.where(
            ProductMarkingCode.status == MarkingCodeStatus.IN_STOCK
        )
    stmt = stmt.order_by(ProductMarkingCode.id.asc()).limit(
        max(1, min(int(limit or 1000), 5000))
    )
    rows = list((await session.execute(stmt)).scalars().all())
    if not rows:
        return {"checked": 0, "matched": 0, "mismatched": []}

    rows_by_code = {str(row.code): row for row in rows}
    codes = list(rows_by_code.keys())
    now = now_moscow()
    checked = 0
    matched = 0
    mismatched: list[dict[str, Any]] = []
    for offset in range(0, len(codes), GIS_CISES_INFO_CHUNK):
        chunk = codes[offset:offset + GIS_CISES_INFO_CHUNK]
        results = await gis_client.get_cises_info(chunk)
        for item in results:
            cis, gis_status = _extract_cis_and_status(item)
            row = rows_by_code.get(cis)
            if row is None:
                continue
            checked += 1
            row.gis_status = gis_status or None
            row.gis_checked_at = now
            our_status = str(
                getattr(row.status, "value", row.status)
            )
            is_ok = (
                our_status != "in_stock"
                or gis_status in GIS_OK_STATUSES
            )
            if is_ok:
                matched += 1
            else:
                mismatched.append(
                    {
                        "id": int(row.id),
                        "code": str(row.code),
                        "our_status": our_status,
                        "gis_status": gis_status or "<нет данных>",
                    }
                )
            session.add(row)
    settings_row.last_check_at = now
    session.add(settings_row)
    await session.commit()
    return {
        "checked": checked,
        "matched": matched,
        "mismatched": mismatched,
    }


def build_withdrawal_product_document(
    *,
    inn: str,
    codes: list[str],
    action: str,
    document_number: str,
    action_date: Optional[str] = None,
    primary_document_name: str = "Внутренний документ",
) -> dict[str, Any]:
    """Тело документа «Вывод из оборота» (LK_RECEIPT, True API)."""
    normalized_action = str(action or "").strip().upper()
    if normalized_action not in WITHDRAWAL_ACTIONS:
        raise ValueError(
            f"Недопустимая причина вывода: {action}. "
            f"Ожидается одна из: {', '.join(WITHDRAWAL_ACTIONS)}"
        )
    if not codes:
        raise ValueError("Не выбраны коды для вывода из оборота")
    date_text = action_date or now_moscow().strftime("%Y-%m-%d")
    return {
        "inn": str(inn or "").strip(),
        "action": normalized_action,
        "action_date": date_text,
        "document_type": "OTHER",
        "document_number": str(document_number or "").strip(),
        "document_date": date_text,
        "primary_document_custom_name": primary_document_name,
        "products": [{"cis": code} for code in codes],
    }


async def start_gis_mt_withdraw_task(
    session: AsyncSession,
    *,
    diadoc_client: DiadocClient,
    environment: str,
    organization_inn: str,
    marking_code_ids: list[int],
    action: str,
    document_number: Optional[str] = None,
) -> DiadocCloudSignTask:
    """Шаг 1 вывода из оборота: документ → CloudSign → SMS."""
    settings_row = await get_or_create_gis_mt_settings(session)
    _require_active_client(settings_row)
    if not str(settings_row.product_group or "").strip():
        raise ValueError(
            "Не указана товарная группа ГИС МТ (product_group) — "
            "заполните её на странице маркировки"
        )
    ids = [int(value) for value in (marking_code_ids or []) if value]
    if not ids:
        raise ValueError("Не выбраны коды для вывода из оборота")
    rows = list(
        (
            await session.execute(
                select(ProductMarkingCode).where(
                    ProductMarkingCode.id.in_(ids),
                    ProductMarkingCode.status.in_(
                        [
                            MarkingCodeStatus.IN_STOCK,
                            MarkingCodeStatus.RECEIVED,
                        ]
                    ),
                )
            )
        )
        .scalars()
        .all()
    )
    if len(rows) != len(set(ids)):
        raise ValueError("Часть выбранных кодов недоступна для вывода из оборота")
    number = str(document_number or "").strip() or f"DZ-WD-{uuid4().hex}"
    document = build_withdrawal_product_document(
        inn=organization_inn,
        codes=[str(row.code) for row in rows],
        action=action,
        document_number=number,
    )
    document_bytes = json.dumps(
        document, ensure_ascii=False
    ).encode("utf-8")
    content_b64 = base64.b64encode(document_bytes).decode("ascii")
    token = await diadoc_client.cloud_sign(
        files=[
            {
                "FileName": f"{number}.json",
                "Content": {"Content": content_b64},
            }
        ]
    )
    task = DiadocCloudSignTask(
        environment=environment,
        operation=OPERATION_GIS_MT_WITHDRAW,
        state="waiting_code",
        box_id_guid="-",
        cloud_sign_token=token,
        files=[
            {
                "file_name": f"{number}.json",
                "content_b64": content_b64,
                "kind": "gis_withdraw_document",
                "parent_entity_id": None,
            }
        ],
        params={
            "marking_code_ids": [int(row.id) for row in rows],
            "product_group": settings_row.product_group,
            "action": str(action).strip().upper(),
            "document_number": number,
        },
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def _withdrawal_codes(session: AsyncSession, ids: list[int]) -> list[ProductMarkingCode]:
    stmt = (
        select(ProductMarkingCode)
        .where(ProductMarkingCode.id.in_(ids))
        .order_by(ProductMarkingCode.id)
        .with_for_update(of=ProductMarkingCode)
        .execution_options(populate_existing=True)
    )
    rows = list((await session.scalars(stmt)).all())
    if not rows or len(rows) != len(set(ids)):
        raise ValueError("Состав выбранных кодов изменился")
    return rows


async def confirm_withdrawal(
    session: AsyncSession, *, task: DiadocCloudSignTask, diadoc_client: DiadocClient, code: str
) -> DiadocCloudSignTask:
    # Serialize SMS confirmations, then reserve every signed code before the network POST.
    await session.refresh(task, with_for_update=True)
    if task.state != "waiting_code":
        raise ValueError("Заявление уже отправлялось. Проверьте результат в журнале маркировки")
    settings = await get_or_create_gis_mt_settings(session)
    client = _require_active_client(settings)
    params = task.params or {}
    group = params.get("product_group") or settings.product_group  # старые задачи подписи
    if not group:
        raise ValueError("Не указана товарная группа ГИС МТ")
    rows = await _withdrawal_codes(session, params["marking_code_ids"])
    if any(
        row.status not in (MarkingCodeStatus.IN_STOCK, MarkingCodeStatus.RECEIVED) for row in rows
    ):
        raise ValueError("Код уже отгружен, зарезервирован или ожидает результата вывода")
    scanned = await session.scalar(
        select(ShipmentMarkingScan.id)
        .join(ShipmentDocument, ShipmentDocument.id == ShipmentMarkingScan.shipment_document_id)
        .where(
            ShipmentMarkingScan.marking_code_id.in_(params["marking_code_ids"]),
            ShipmentDocument.status == ShipmentDocumentStatus.DRAFT,
        )
        .limit(1)
    )
    if scanned is not None:
        raise ValueError("Код уже отсканирован для сборки отгрузки")
    content = task.files[0]["content_b64"]
    original = json.loads(base64.b64decode(content))
    if sorted(item["cis"] for item in original["products"]) != sorted(row.code for row in rows):
        raise ValueError("Состав кодов отличается от подписываемого заявления")
    signatures = await diadoc_client.cloud_sign_confirm(
        token=task.cloud_sign_token, confirmation_code=code
    )
    if len(signatures) != 1 or not signatures[0]:
        raise ValueError("Диадок не вернул подпись заявления")
    task.params = {
        **params,
        "product_group": group,
        "original_status": {str(row.id): row.status.value for row in rows},
    }
    for row in rows:
        row.status = MarkingCodeStatus.WITHDRAWAL_PENDING
    task.state = "gis_unknown"
    task.cloud_sign_token = None
    task.error_details = (
        "Отправка начата. Если ID не получен, сверьте заявление в личном кабинете ГИС МТ"
    )
    await session.commit()
    try:
        doc_id = await client.create_document(
            product_group=group,
            document_type="LK_RECEIPT",
            product_document_b64=content,
            signature_b64=signatures[0],
        )
    except GisMtApiError as exc:
        if exc.status_code in (400, 401, 403, 422):
            await session.refresh(task, with_for_update=True)
            if task.state == "gis_unknown":
                await _finish_withdrawal(session, task, accepted=False, error=exc.detail)
                await session.commit()
            return task
        logger.warning("Unconfirmed GIS submission for task %s: %s", task.id, exc)
        return task
    except (httpx.RequestError, ValueError) as exc:
        logger.warning("Unconfirmed GIS submission for task %s: %s", task.id, exc)
        return task
    if not doc_id:
        return task
    await session.refresh(task, with_for_update=True)
    if task.state == "gis_unknown":
        task.params = {**task.params, "gis_document_id": doc_id}
        task.state = "gis_pending"
        task.files = []
        task.error_details = None
        await session.commit()
    return task


async def _finish_withdrawal(
    session: AsyncSession, task: DiadocCloudSignTask, *, accepted: bool, error: str | None = None
) -> None:
    params = task.params
    rows = await _withdrawal_codes(session, params["marking_code_ids"])
    if any(row.status != MarkingCodeStatus.WITHDRAWAL_PENDING for row in rows):
        raise ValueError("Коды заявления требуют сверки со складским учётом")
    now = now_moscow()
    for row in rows:
        if accepted:
            row.status = MarkingCodeStatus.WITHDRAWN
            row.withdrawn_at = now
            session.add(
                ProductMarkingCodeMovement(
                    marking_code_id=row.id,
                    movement_type=MarkingMovementType.WITHDRAWN,
                    autopart_id=row.autopart_id,
                    stock_lot_id=row.stock_lot_id,
                    metadata_json={
                        "source": "gis_mt_withdraw",
                        "gis_document_id": params["gis_document_id"],
                        "document_number": params["document_number"],
                        "action": params["action"],
                    },
                )
            )
        else:
            row.status = MarkingCodeStatus(params["original_status"][str(row.id)])
    task.state = "completed" if accepted else "gis_rejected"
    task.error_details = str(error or "")[:2000] or None
    task.completed_at = now
    task.files = []


async def refresh_withdrawal(
    session: AsyncSession, task_id: int, recovered_id: str | None = None
) -> DiadocCloudSignTask:
    task = await session.get(
        DiadocCloudSignTask, task_id, with_for_update=True, populate_existing=True
    )
    if task is None or task.operation != OPERATION_GIS_MT_WITHDRAW:
        raise ValueError("Заявление не найдено")
    if task.state not in ("gis_pending", "gis_unknown"):
        return task
    missing_id = task.state == "gis_unknown"
    doc_id = (recovered_id if missing_id else task.params["gis_document_id"]) or ""
    if not doc_id:
        raise ValueError("Укажите ID заявления из личного кабинета ГИС МТ")
    client = _require_active_client(await get_or_create_gis_mt_settings(session))
    info = await client.withdrawal_info(doc_id, task.params["product_group"], content=missing_id)
    if missing_id:
        linked_task = await session.scalar(
            select(DiadocCloudSignTask.id)
            .where(
                DiadocCloudSignTask.operation == OPERATION_GIS_MT_WITHDRAW,
                DiadocCloudSignTask.id != task_id,
                DiadocCloudSignTask.params["gis_document_id"].as_string() == doc_id,
            )
            .limit(1)
        )
        if linked_task is not None:
            raise ValueError("Этот ID ГИС МТ уже связан с другим заявлением")
        actual = info.get("content")
        if isinstance(actual, str):
            actual = json.loads(actual)
        expected = json.loads(base64.b64decode(task.files[0]["content_b64"]))
        if actual != expected:
            raise ValueError("Содержимое документа ГИС МТ отличается от подписанного заявления")
    status = info.get("status")
    task.params = {**task.params, "gis_document_id": doc_id, "gis_status": status}
    task.files = []
    task.state = "gis_pending"
    task.error_details = None
    if status == "CHECKED_OK":
        await _finish_withdrawal(session, task, accepted=True)
    elif status in {"CHECKED_NOT_OK", "PARSE_ERROR"}:
        errors = info.get("errors") or info.get("commonErrors") or status
        await _finish_withdrawal(
            session, task, accepted=False, error=json.dumps(errors, ensure_ascii=False)
        )
    elif status == "PROCESSING_ERROR":
        task.error_details = (
            "ГИС МТ сообщила о технической ошибке обработки. Ожидается окончательный результат"
        )
    task.updated_at = now_moscow()
    await session.commit()
    return task


async def poll_withdrawals(session: AsyncSession) -> None:
    settings = await session.scalar(select(GisMtSettings).limit(1))
    if settings is None or not gis_mt_token_is_active(settings):
        return
    ids = list(
        await session.scalars(
            select(DiadocCloudSignTask.id)
            .where(
                DiadocCloudSignTask.operation == OPERATION_GIS_MT_WITHDRAW,
                DiadocCloudSignTask.state == "gis_pending",
            )
            .order_by(DiadocCloudSignTask.updated_at, DiadocCloudSignTask.id)
            .limit(25)
        )
    )
    for task_id in ids:
        try:
            await refresh_withdrawal(session, task_id)
        except Exception as exc:
            await session.rollback()
            task = await session.get(
                DiadocCloudSignTask, task_id, with_for_update=True, populate_existing=True
            )
            if task and task.state == "gis_pending":
                task.error_details = str(exc)[:2000]
                task.updated_at = now_moscow()
            await session.commit()
            logger.warning("GIS MT withdrawal #%s status check failed: %s", task_id, exc)


async def recent_withdrawals(session: AsyncSession) -> list[dict[str, Any]]:
    stmt = (
        select(DiadocCloudSignTask)
        .where(
            DiadocCloudSignTask.operation == OPERATION_GIS_MT_WITHDRAW,
            DiadocCloudSignTask.params["original_status"].as_string().is_not(None),
        )
        .order_by(
            DiadocCloudSignTask.state.in_(("gis_unknown", "gis_pending")).desc(),
            DiadocCloudSignTask.updated_at.desc(),
        )
        .limit(100)
    )
    return [
        {
            "id": task.id,
            "state": task.state,
            "number": task.params.get("document_number"),
            "document_id": task.params.get("gis_document_id"),
            "gis_status": task.params.get("gis_status"),
            "error": task.error_details,
        }
        for task in await session.scalars(stmt)
    ]


LOGIN_REMINDER_TITLE = "Нужен вход в ГИС МТ"
LOGIN_REMINDER_LINK = "/warehouse/marking"


async def _pending_marking_work(session: AsyncSession) -> dict[str, int]:
    """Документы, для которых понадобится сверка с ГИС МТ."""
    from dz_fastapi.models.inventory import ShipmentDocumentItem
    from dz_fastapi.models.partner import SupplierReceipt, SupplierReceiptItem

    receipts = await session.scalar(
        select(func.count(func.distinct(SupplierReceipt.id)))
        .join(SupplierReceiptItem, SupplierReceiptItem.receipt_id == SupplierReceipt.id)
        .where(
            SupplierReceipt.posted_at.is_(None),
            SupplierReceiptItem.marking_codes.is_not(None),
            cast(SupplierReceiptItem.marking_codes, Text).notin_(("null", "[]")),
        )
    )
    marked_autoparts = select(ProductMarkingCode.autopart_id).where(
        ProductMarkingCode.autopart_id.is_not(None)
    )
    shipments = await session.scalar(
        select(func.count(func.distinct(ShipmentDocument.id)))
        .join(ShipmentDocumentItem, ShipmentDocumentItem.document_id == ShipmentDocument.id)
        .where(
            ShipmentDocument.status == ShipmentDocumentStatus.DRAFT,
            ShipmentDocumentItem.autopart_id.in_(marked_autoparts),
        )
    )
    withdrawals = await session.scalar(
        select(func.count(DiadocCloudSignTask.id)).where(
            DiadocCloudSignTask.operation == OPERATION_GIS_MT_WITHDRAW,
            DiadocCloudSignTask.state.in_(("gis_pending", "gis_unknown")),
        )
    )
    return {
        "pending_withdrawals": int(withdrawals or 0),
        "pending_receipts": int(receipts or 0),
        "pending_shipments": int(shipments or 0),
    }


async def remind_gis_mt_login(session: AsyncSession) -> dict[str, Any]:
    """Напоминает администраторам войти в ГИС МТ, если есть что сверять.

    Токен живёт около 10 часов и получается по SMS, поэтому сам не
    продлевается. Напоминание — не чаще раза в день и только когда вход уже
    использовался, а в работе есть документы с кодами маркировки.
    """
    from dz_fastapi.models.notification import AppNotification, AppNotificationLevel
    from dz_fastapi.services.notifications import create_admin_notifications

    settings_row = (
        await session.execute(select(GisMtSettings).limit(1))
    ).scalars().first()
    result: dict[str, Any] = {"reminded": False}
    if settings_row is None or settings_row.token_expires_at is None:
        result["reason"] = "never_logged_in"
        return result
    if gis_mt_token_is_active(settings_row):
        result["reason"] = "token_active"
        return result
    pending = await _pending_marking_work(session)
    result.update(pending)
    if not any(pending.values()):
        result["reason"] = "nothing_pending"
        return result
    today = now_moscow().replace(hour=0, minute=0, second=0, microsecond=0)
    already = await session.scalar(
        select(func.count(AppNotification.id)).where(
            AppNotification.title == LOGIN_REMINDER_TITLE,
            AppNotification.created_at >= today,
        )
    )
    if already:
        result["reason"] = "already_reminded_today"
        return result
    parts = []
    if pending["pending_withdrawals"]:
        parts.append(f"заявлений на вывод на проверке: {pending['pending_withdrawals']}")
    if pending["pending_receipts"]:
        parts.append(f"поступлений с кодами к проведению: {pending['pending_receipts']}")
    if pending["pending_shipments"]:
        parts.append(f"отгрузок маркируемого товара в работе: {pending['pending_shipments']}")
    await create_admin_notifications(
        session,
        title=LOGIN_REMINDER_TITLE,
        message=(
            "Токен ГИС МТ истёк, коды маркировки не сверяются с Честным "
            "знаком. В работе: " + "; ".join(parts) + "."
        ),
        level=AppNotificationLevel.WARNING,
        link=LOGIN_REMINDER_LINK,
    )
    result["reminded"] = True
    return result


async def get_gis_mt_status(session: AsyncSession) -> dict[str, Any]:
    settings_row = await get_or_create_gis_mt_settings(session)
    return {
        "withdrawals": await recent_withdrawals(session),
        "token_active": gis_mt_token_is_active(settings_row),
        "token_expires_at": settings_row.token_expires_at,
        "product_group": settings_row.product_group,
        "last_check_at": settings_row.last_check_at,
        "last_error": settings_row.last_error,
        **await _pending_marking_work(session),
        "shipment_codes_auto_fifo": bool(
            True
            if settings_row.shipment_codes_auto_fifo is None
            else settings_row.shipment_codes_auto_fifo
        ),
    }


async def update_shipment_codes_auto_fifo(
    session: AsyncSession, *, enabled: bool
) -> dict[str, Any]:
    settings_row = await get_or_create_gis_mt_settings(session)
    settings_row.shipment_codes_auto_fifo = bool(enabled)
    session.add(settings_row)
    await session.commit()
    return await get_gis_mt_status(session)


async def update_gis_mt_product_group(
    session: AsyncSession,
    *,
    product_group: str,
) -> dict[str, Any]:
    settings_row = await get_or_create_gis_mt_settings(session)
    settings_row.product_group = (
        str(product_group or "").strip() or None
    )
    session.add(settings_row)
    await session.commit()
    return await get_gis_mt_status(session)
