"""API очереди исходящих писем (EmailOutbox).

Контракт для внешнего скрипта-релея (машина с открытыми SMTP-портами):
1. GET  /email-outbox/pending      — забрать письма к отправке;
2. отправить через smtp.yandex.ru:465 с нужного from-адреса;
3. POST /email-outbox/{id}/mark-sent  или  /mark-error.
Плюс GET /email-outbox — недавние письма для UI.
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import get_current_user
from dz_fastapi.core.db import get_session
from dz_fastapi.models.partner import EmailOutbox
from dz_fastapi.schemas.reclamation import EmailOutboxOut, OutboxMarkErrorIn
from dz_fastapi.services.email_outbox import (
    claim_pending_outbox,
    list_pending_outbox,
    mark_outbox_error,
    mark_outbox_sent,
)

logger = logging.getLogger("dz_fastapi")

router = APIRouter(
    prefix="/email-outbox",
    tags=["email-outbox"],
    dependencies=[Depends(get_current_user)],
)


@router.get(
    "/pending",
    response_model=list[EmailOutboxOut],
    summary="Письма, ожидающие отправки (для релея)",
)
async def outbox_pending(
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    return await list_pending_outbox(session, limit=limit)


@router.post(
    "/claim",
    response_model=list[EmailOutboxOut],
    summary="Атомарно захватить письма для отправки (для релея)",
)
async def outbox_claim(
    worker: str = Query(..., min_length=1, max_length=128),
    limit: int = Query(default=25, ge=1, le=200),
    lease_seconds: int = Query(default=300, ge=30, le=3600),
    session: AsyncSession = Depends(get_session),
):
    return await claim_pending_outbox(
        session,
        worker=worker,
        limit=limit,
        lease_seconds=lease_seconds,
    )


@router.get(
    "",
    response_model=list[EmailOutboxOut],
    summary="Последние письма очереди",
)
async def outbox_list(
    status_filter: Optional[str] = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = (
        select(EmailOutbox)
        .order_by(EmailOutbox.id.desc())
        .limit(limit)
    )
    if status_filter:
        stmt = stmt.where(EmailOutbox.status == status_filter)
    rows = (await session.execute(stmt)).scalars().all()
    return list(rows)


@router.post(
    "/{outbox_id}/mark-sent",
    response_model=EmailOutboxOut,
    summary="Отметить письмо как отправленное",
)
async def outbox_mark_sent(
    outbox_id: int,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await mark_outbox_sent(session, outbox_id=outbox_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/{outbox_id}/mark-error",
    response_model=EmailOutboxOut,
    summary="Отметить ошибку отправки",
)
async def outbox_mark_error(
    outbox_id: int,
    payload: OutboxMarkErrorIn,
    session: AsyncSession = Depends(get_session),
):
    try:
        return await mark_outbox_error(
            session,
            outbox_id=outbox_id,
            error=payload.error,
            retry=payload.retry,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
