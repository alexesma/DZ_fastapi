"""Обмен с 1С.

`/1c/exchange` — стандартный протокол «Обмен с сайтом» (его штатно
понимает 1С:УТ/УНФ): 1С сама ходит по расписанию и забирает проведённые
отгрузки как документы CommerceML. Авторизация — логин/пароль из env
(ONE_C_EXCHANGE_LOGIN / ONE_C_EXCHANGE_PASSWORD) + подписанная cookie.

Остальные эндпоинты — ручные выгрузки для бухгалтерии (Excel/XML) и
управление очередью, доступны только админам.
"""

import asyncio
import base64
import hashlib
import hmac
import logging
import os
import time
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.config import settings
from dz_fastapi.core.db import get_session
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.one_c import (
    OneCExchangeBatch,
    OneCExchangeBatchItem,
    OneCExchangeEvent,
    OneCExchangeEventStatus,
)
from dz_fastapi.models.user import User
from dz_fastapi.schemas.one_c import (
    ONE_C_OUTBOX_CONTRACT_VERSION,
    OneCBatchAckRequest,
    OneCOutboxEnvelope,
    OneCOutboxPingResponse,
)
from dz_fastapi.services.one_c_exchange import (
    build_commerceml_sale_xml,
    build_commerceml_sale_xml_from_snapshots,
    build_counterparties_xlsx,
    build_nomenclature_xlsx,
    build_receipts_xlsx,
    build_shipments_xlsx,
    get_one_c_exchange_status,
    get_sales_history_summary,
    import_sales_history_xlsx,
    list_shipments_for_1c,
    reset_shipments_sync_status,
)
from dz_fastapi.services.one_c_outbox import (
    CHANNEL_COMMERCEML,
    CHANNEL_JSON,
    ENTITY_PRODUCTION_WAVE,
    ENTITY_RECEIPT,
    ENTITY_SHIPMENT,
    ENTITY_STOCK_DOCUMENT,
    EVENT_POSTED,
    acknowledge_delivery_batch,
    acknowledge_latest_batch,
    backfill_pending_shipment_events,
    get_one_c_outbox_status,
    get_or_create_delivery_batch,
    retry_one_c_event,
    serialize_one_c_batch,
    serialize_one_c_event,
)

logger = logging.getLogger("dz_fastapi")

router = APIRouter(prefix="/1c", tags=["1c"])

ONE_C_COOKIE_NAME = "dz1c"
ONE_C_COOKIE_TTL_SECONDS = 24 * 3600
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _one_c_credentials() -> tuple[str, str]:
    login = str(os.getenv("ONE_C_EXCHANGE_LOGIN") or "").strip()
    password = str(os.getenv("ONE_C_EXCHANGE_PASSWORD") or "").strip()
    return login, password


def _sign_cookie_value(payload: str) -> str:
    digest = hmac.new(
        settings.jwt_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _build_session_cookie() -> str:
    timestamp = str(int(time.time()))
    return f"{timestamp}.{_sign_cookie_value(timestamp)}"


def _cookie_is_valid(value: str) -> bool:
    try:
        timestamp_str, signature = str(value or "").split(".", 1)
    except ValueError:
        return False
    if not hmac.compare_digest(signature, _sign_cookie_value(timestamp_str)):
        return False
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        return False
    return time.time() - timestamp <= ONE_C_COOKIE_TTL_SECONDS


def _basic_auth_is_valid(request: Request) -> bool:
    login, password = _one_c_credentials()
    if not login or not password:
        return False
    header = str(request.headers.get("Authorization") or "")
    if not header.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:]).decode("utf-8")
        got_login, got_password = decoded.split(":", 1)
    except Exception:  # noqa: BLE001
        return False
    return hmac.compare_digest(got_login, login) and hmac.compare_digest(got_password, password)


def _request_is_authorized(request: Request) -> bool:
    if _basic_auth_is_valid(request):
        return True
    return _cookie_is_valid(request.cookies.get(ONE_C_COOKIE_NAME, ""))


@router.api_route(
    "/exchange",
    methods=["GET", "POST"],
    include_in_schema=False,
)
async def one_c_exchange_protocol(
    request: Request,
    type: str = Query(default=""),
    mode: str = Query(default=""),
    session: AsyncSession = Depends(get_session),
):
    """Протокол «Обмен с сайтом»: checkauth → init → query → success."""
    login, password = _one_c_credentials()
    if not login or not password:
        return PlainTextResponse(
            "failure\nОбмен с 1С не настроен: задайте "
            "ONE_C_EXCHANGE_LOGIN и ONE_C_EXCHANGE_PASSWORD",
            status_code=503,
        )
    exchange_type = str(type or "").strip().lower()
    exchange_mode = str(mode or "").strip().lower()

    if exchange_mode == "checkauth":
        if not _basic_auth_is_valid(request):
            return PlainTextResponse("failure\nНеверный логин или пароль")
        cookie_value = _build_session_cookie()
        return PlainTextResponse(f"success\n{ONE_C_COOKIE_NAME}\n{cookie_value}")

    if not _request_is_authorized(request):
        return PlainTextResponse("failure\nНе авторизовано", status_code=401)

    if exchange_mode == "init":
        return PlainTextResponse("zip=no\nfile_limit=10000000")

    if exchange_type == "sale" and exchange_mode == "query":
        await backfill_pending_shipment_events(session)
        batch = await get_or_create_delivery_batch(
            session,
            channel=CHANNEL_COMMERCEML,
            entity_types=[ENTITY_SHIPMENT],
            event_types=[EVENT_POSTED],
            limit=100,
        )
        snapshots = [item.event.payload for item in batch.items] if batch is not None else []
        xml_payload = build_commerceml_sale_xml_from_snapshots(
            snapshots,
            formed_at=batch.sent_at if batch else None,
        )
        await session.commit()
        logger.info(
            "1C exchange: sent batch=%s shipments=%s attempt=%s",
            batch.batch_uid if batch else None,
            len(snapshots),
            batch.attempt_count if batch else 0,
        )
        return Response(
            content=xml_payload,
            media_type="application/xml; charset=utf-8",
            headers=({"X-DZ-1C-Batch-ID": batch.batch_uid} if batch else {}),
        )

    if exchange_mode == "success":
        batch = await acknowledge_latest_batch(session, channel=CHANNEL_COMMERCEML)
        await session.commit()
        logger.info(
            "1C exchange: confirmed batch=%s",
            batch.batch_uid if batch else None,
        )
        return PlainTextResponse("success")

    if exchange_mode == "file":
        # Двусторонний обмен каталогом нам не нужен — файл из 1С
        # принимаем и игнорируем, чтобы настройка в 1С не падала.
        body = await request.body()
        logger.info(
            "1C exchange: ignored inbound %s file (%s bytes)",
            exchange_type or "<no type>",
            len(body),
        )
        return PlainTextResponse("success")

    if exchange_mode in ("import", "complete", "deactivate"):
        return PlainTextResponse("success")

    return PlainTextResponse(f"failure\nНеизвестный режим: {exchange_mode}", status_code=400)


def _xlsx_response(content: bytes, filename: str) -> Response:
    return Response(
        content=content,
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/status", summary="Статус обмена с 1С")
async def one_c_status(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    login, password = _one_c_credentials()
    counters = await get_one_c_exchange_status(session)
    outbox = await get_one_c_outbox_status(session)
    return {
        "configured": bool(login and password),
        "login": login or None,
        **counters,
        **outbox,
    }


@router.get("/events", summary="Журнал событий обмена с 1С")
async def list_one_c_events(
    event_status: Optional[OneCExchangeEventStatus] = Query(default=None, alias="status"),
    entity_type: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    stmt = select(OneCExchangeEvent).order_by(
        OneCExchangeEvent.created_at.desc(), OneCExchangeEvent.id.desc()
    )
    if event_status:
        stmt = stmt.where(OneCExchangeEvent.status == event_status)
    if entity_type:
        stmt = stmt.where(OneCExchangeEvent.entity_type == entity_type)
    rows = (await session.execute(stmt.limit(limit))).scalars().all()
    return {"items": [serialize_one_c_event(row, include_payload=False) for row in rows]}


@router.get("/batches", summary="Пакеты передачи в 1С")
async def list_one_c_batches(
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    rows = (
        (
            await session.execute(
                select(OneCExchangeBatch)
                .options(
                    selectinload(OneCExchangeBatch.items).selectinload(OneCExchangeBatchItem.event)
                )
                .order_by(
                    OneCExchangeBatch.created_at.desc(),
                    OneCExchangeBatch.id.desc(),
                )
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return {"items": [serialize_one_c_batch(row) for row in rows]}


@router.post("/events/{event_id}/retry", summary="Повторить событие обмена")
async def retry_one_c_event_endpoint(
    event_id: int,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    try:
        event = await retry_one_c_event(session, event_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await session.commit()
    return serialize_one_c_event(event, include_payload=False)


def _one_c_outbox_response(batch: Optional[OneCExchangeBatch]) -> dict:
    if batch is None:
        return {
            "protocol": "dz-1c-outbox",
            "contract_version": ONE_C_OUTBOX_CONTRACT_VERSION,
            "batch_uid": None,
            "content_hash": None,
            "formed_at": None,
            "attempt": 0,
            "events": [],
        }
    return {
        "protocol": "dz-1c-outbox",
        "contract_version": ONE_C_OUTBOX_CONTRACT_VERSION,
        "batch_uid": batch.batch_uid,
        "content_hash": batch.content_hash,
        "formed_at": batch.sent_at,
        "attempt": batch.attempt_count,
        "events": [serialize_one_c_event(item.event) for item in batch.items],
    }


@router.get(
    "/outbox/v1/ping",
    summary="Проверить подключение адаптера 1С",
    response_model=OneCOutboxPingResponse,
)
async def ping_one_c_outbox(request: Request):
    if not _request_is_authorized(request):
        raise HTTPException(status_code=401, detail="Не авторизовано")
    return {
        "protocol": "dz-1c-outbox",
        "contract_version": ONE_C_OUTBOX_CONTRACT_VERSION,
        "status": "ok",
        "server_time": now_moscow(),
    }


@router.get(
    "/outbox/query",
    summary="Получить идемпотентный JSON-пакет для расширения 1С",
    response_model=OneCOutboxEnvelope,
)
@router.get(
    "/outbox/v1/query",
    summary="Получить JSON-пакет DZ-1C v1",
    response_model=OneCOutboxEnvelope,
)
async def query_one_c_outbox(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    if not _request_is_authorized(request):
        raise HTTPException(status_code=401, detail="Не авторизовано")
    await backfill_pending_shipment_events(session)
    batch = await get_or_create_delivery_batch(
        session,
        channel=CHANNEL_JSON,
        entity_types=[
            ENTITY_SHIPMENT,
            ENTITY_RECEIPT,
            ENTITY_STOCK_DOCUMENT,
            ENTITY_PRODUCTION_WAVE,
        ],
        limit=limit,
    )
    await session.commit()
    return _one_c_outbox_response(batch)


@router.post(
    "/outbox/{batch_uid}/ack",
    summary="Подтвердить обработку JSON-пакета расширением 1С",
)
@router.post(
    "/outbox/v1/{batch_uid}/ack",
    summary="Подтвердить обработку пакета DZ-1C v1",
)
async def acknowledge_one_c_outbox(
    batch_uid: str,
    payload: OneCBatchAckRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
):
    if not _request_is_authorized(request):
        raise HTTPException(status_code=401, detail="Не авторизовано")
    try:
        batch = await acknowledge_delivery_batch(
            session,
            batch_uid=batch_uid,
            success=payload.success,
            error=payload.error,
            external_ids=payload.external_ids,
            event_results={result.event_uid: result.model_dump() for result in payload.results}
            or None,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await session.commit()
    return serialize_one_c_batch(batch)


@router.get(
    "/export/shipments.xlsx",
    summary="Выгрузка реализаций (Excel)",
)
async def export_shipments_xlsx(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    only_pending: bool = Query(default=False),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    content = await build_shipments_xlsx(
        session,
        date_from=date_from,
        date_to=date_to,
        only_pending=only_pending,
    )
    return _xlsx_response(content, "realizatsii_1c.xlsx")


@router.get(
    "/export/shipments.xml",
    summary="Выгрузка реализаций (CommerceML XML)",
)
async def export_shipments_xml(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    only_pending: bool = Query(default=False),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    shipments = await list_shipments_for_1c(
        session,
        only_pending=only_pending,
        date_from=date_from,
        date_to=date_to,
    )
    # Экспорт без лимита может быть большим — строим XML вне event loop.
    xml_payload = await asyncio.to_thread(build_commerceml_sale_xml, shipments)
    return Response(
        content=xml_payload,
        media_type="application/xml; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="orders_1c.xml"'},
    )


@router.get(
    "/export/receipts.xlsx",
    summary="Выгрузка поступлений (Excel)",
)
async def export_receipts_xlsx(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    content = await build_receipts_xlsx(session, date_from=date_from, date_to=date_to)
    return _xlsx_response(content, "postupleniya_1c.xlsx")


@router.get(
    "/export/counterparties.xlsx",
    summary="Выгрузка контрагентов (Excel)",
)
async def export_counterparties_xlsx(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    content = await build_counterparties_xlsx(session)
    return _xlsx_response(content, "kontragenty_1c.xlsx")


@router.get(
    "/export/nomenclature.xlsx",
    summary="Выгрузка номенклатуры (Excel)",
)
async def export_nomenclature_xlsx(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    content = await build_nomenclature_xlsx(session)
    return _xlsx_response(content, "nomenklatura_1c.xlsx")


@router.post(
    "/reset-export",
    summary="Вернуть отгрузки периода в очередь выгрузки в 1С",
)
async def reset_export(
    date_from: Optional[date] = Query(default=None),
    date_to: Optional[date] = Query(default=None),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    if date_from is None and date_to is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Укажите период (date_from и/или date_to)",
        )
    count = await reset_shipments_sync_status(session, date_from=date_from, date_to=date_to)
    return {"reset": count}


@router.get(
    "/sales-history/summary",
    summary="Сводка загруженной истории продаж из 1С",
)
async def sales_history_summary(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    return await get_sales_history_summary(session)


@router.post(
    "/sales-history/import",
    summary="Загрузка истории продаж из Excel (выгрузка 1С)",
)
async def sales_history_import(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_admin),
):
    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Файл пустой")
    try:
        return await import_sales_history_xlsx(session, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=400,
            detail=f"Не удалось разобрать файл: {exc}",
        ) from exc
