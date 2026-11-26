"""API журнала исходящих писем: что ушло, что повторяется, что не ушло."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.api.deps import require_admin
from dz_fastapi.core.db import get_session
from dz_fastapi.models.email_send_log import EmailSendLog
from dz_fastapi.services.email_retry import schedule_manual_retry

router = APIRouter(prefix="/email-log", tags=["email-log"])


class EmailLogOut(BaseModel):
    id: int
    created_at: Optional[str] = None
    to_email: str
    from_email: Optional[str] = None
    subject: Optional[str] = None
    transport: Optional[str] = None
    source: Optional[str] = None
    source_id: Optional[int] = None
    attachment_name: Optional[str] = None
    attachment_size: Optional[int] = None
    status: str
    attempts: int
    next_retry_at: Optional[str] = None
    last_error: Optional[str] = None
    sent_at: Optional[str] = None
    events: list = []
    can_retry: bool = False
    duration_ms: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)


def _iso(value):
    return value.isoformat() if value is not None else None


def _serialize(row: EmailSendLog) -> EmailLogOut:
    return EmailLogOut(
        id=row.id,
        created_at=_iso(row.created_at),
        to_email=row.to_email,
        from_email=row.from_email,
        subject=row.subject,
        transport=row.transport,
        source=row.source,
        source_id=row.source_id,
        attachment_name=row.attachment_name,
        attachment_size=row.attachment_size,
        status=row.status,
        attempts=row.attempts or 0,
        next_retry_at=_iso(row.next_retry_at),
        last_error=row.last_error,
        sent_at=_iso(row.sent_at),
        events=list(row.events or []),
        can_retry=bool(
            row.status == "failed"
            and row.retry_allowed
            and (row.body or row.attachment_path)
        ),
        duration_ms=row.duration_ms,
    )


@router.get("", summary="Журнал исходящих писем")
async def list_email_log(
    status: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    _=Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    filters = []
    if status:
        filters.append(EmailSendLog.status == status)
    if source:
        filters.append(EmailSendLog.source == source)
    if q:
        like = f"%{q.strip()}%"
        filters.append(
            or_(EmailSendLog.to_email.ilike(like), EmailSendLog.subject.ilike(like))
        )
    total = (
        await session.execute(select(func.count(EmailSendLog.id)).where(*filters))
    ).scalar_one()
    rows = (
        await session.execute(
            select(EmailSendLog)
            .where(*filters)
            .order_by(EmailSendLog.created_at.desc(), EmailSendLog.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).scalars().all()
    counters = dict(
        (
            await session.execute(
                select(EmailSendLog.status, func.count(EmailSendLog.id)).group_by(
                    EmailSendLog.status
                )
            )
        ).all()
    )
    return {
        "items": [_serialize(row) for row in rows],
        "total": total,
        "counters": {key: int(counters.get(key, 0)) for key in ("sent", "retry", "failed")},
    }


@router.post("/{log_id}/retry", summary="Повторить неушедшее письмо")
async def retry_email(
    log_id: int,
    _=Depends(require_admin),
    session: AsyncSession = Depends(get_session),
):
    row = await session.get(EmailSendLog, log_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    try:
        await schedule_manual_retry(session, row)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _serialize(row)
