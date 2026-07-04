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

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.http.diadoc_client import DiadocClient
from dz_fastapi.http.gis_mt_client import GisMtClient
from dz_fastapi.models.diadoc import DiadocCloudSignTask
from dz_fastapi.models.inventory import (
    MarkingCodeStatus,
    MarkingMovementType,
    ProductMarkingCode,
    ProductMarkingCodeMovement,
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
        ).scalars().all()
    )
    if not rows:
        raise ValueError(
            "Из выбранных кодов ни один не находится на складе"
        )
    number = (
        str(document_number or "").strip()
        or f"DZ-WD-{now_moscow().strftime('%Y%m%d-%H%M%S')}"
    )
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
            "action": str(action).strip().upper(),
            "document_number": number,
        },
    )
    session.add(task)
    await session.commit()
    await session.refresh(task)
    return task


async def apply_gis_mt_withdraw(
    session: AsyncSession,
    *,
    task: DiadocCloudSignTask,
    signatures: list[str],
) -> dict[str, Any]:
    """Шаг 2 вывода: подпись → lk/documents/create → пометить коды."""
    settings_row = await get_or_create_gis_mt_settings(session)
    gis_client = _require_active_client(settings_row)
    params = task.params or {}
    files = list(task.files or [])
    if not files:
        raise ValueError("В задаче нет документа вывода из оборота")
    document_id = await gis_client.create_document(
        product_group=str(settings_row.product_group),
        document_type="LK_RECEIPT",
        product_document_b64=str(files[0]["content_b64"]),
        signature_b64=signatures[0],
    )
    ids = [
        int(value)
        for value in (params.get("marking_code_ids") or [])
        if value
    ]
    now = now_moscow()
    withdrawn = 0
    if ids:
        rows = (
            await session.execute(
                select(ProductMarkingCode).where(
                    ProductMarkingCode.id.in_(ids)
                )
            )
        ).scalars().all()
        for row in rows:
            row.status = MarkingCodeStatus.WITHDRAWN
            row.withdrawn_at = now
            session.add(row)
            session.add(
                ProductMarkingCodeMovement(
                    marking_code_id=row.id,
                    movement_type=MarkingMovementType.WITHDRAWN,
                    autopart_id=row.autopart_id,
                    stock_lot_id=row.stock_lot_id,
                    metadata_json={
                        "source": "gis_mt_withdraw",
                        "gis_document_id": document_id,
                        "action": params.get("action"),
                        "document_number": params.get(
                            "document_number"
                        ),
                    },
                )
            )
            withdrawn += 1
    await session.flush()
    logger.info(
        "GIS MT withdrawal document %s created, %s codes withdrawn",
        document_id,
        withdrawn,
    )
    return {"gis_document_id": document_id, "withdrawn": withdrawn}


async def get_gis_mt_status(session: AsyncSession) -> dict[str, Any]:
    settings_row = await get_or_create_gis_mt_settings(session)
    return {
        "token_active": gis_mt_token_is_active(settings_row),
        "token_expires_at": settings_row.token_expires_at,
        "product_group": settings_row.product_group,
        "last_check_at": settings_row.last_check_at,
        "last_error": settings_row.last_error,
    }


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
