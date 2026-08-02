"""Protected Telegram queue API for the external relay worker."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_email_relay
from dz_fastapi.core.db import get_session
from dz_fastapi.schemas.telegram_outbox import (
    RelayTelegramOutboxOut,
    TelegramOutboxMarkErrorIn,
    TelegramOutboxOut,
)
from dz_fastapi.services.telegram_outbox import (
    claim_pending_telegram_outbox,
    list_pending_telegram_outbox,
    mark_telegram_outbox_error,
    mark_telegram_outbox_sent,
    serialize_telegram_outbox_for_relay,
)

router = APIRouter(prefix="/telegram-outbox", tags=["telegram-outbox"])


@router.get("/pending", response_model=list[RelayTelegramOutboxOut])
async def telegram_outbox_pending(
    limit: int = Query(default=50, ge=1, le=200),
    _: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    rows = await list_pending_telegram_outbox(session, limit=limit)
    return [serialize_telegram_outbox_for_relay(row) for row in rows]


@router.post("/claim", response_model=list[RelayTelegramOutboxOut])
async def telegram_outbox_claim(
    worker: str = Query(..., min_length=1, max_length=128),
    limit: int = Query(default=25, ge=1, le=200),
    lease_seconds: int = Query(default=300, ge=30, le=3600),
    _: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    rows = await claim_pending_telegram_outbox(
        session,
        worker=worker,
        limit=limit,
        lease_seconds=lease_seconds,
    )
    return [serialize_telegram_outbox_for_relay(row) for row in rows]


@router.post(
    "/{outbox_id}/mark-sent",
    response_model=TelegramOutboxOut,
)
async def telegram_outbox_mark_sent(
    outbox_id: int,
    _: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await mark_telegram_outbox_sent(
            session,
            outbox_id=outbox_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/{outbox_id}/mark-error",
    response_model=TelegramOutboxOut,
)
async def telegram_outbox_mark_error(
    outbox_id: int,
    payload: TelegramOutboxMarkErrorIn,
    _: None = Depends(require_email_relay),
    session: AsyncSession = Depends(get_session),
):
    try:
        return await mark_telegram_outbox_error(
            session,
            outbox_id=outbox_id,
            error=payload.error,
            retry=payload.retry,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
