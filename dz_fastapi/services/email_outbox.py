"""Очередь исходящих писем (EmailOutbox).

На проде исходящие SMTP-порты закрыты хостером, а ответы клиентам должны
уходить С адреса Яндекс-ящика (требование клиента). Поэтому письма кладутся
в очередь, а внешний скрипт-релей (на машине с открытыми портами) забирает
их по HTTPS и отправляет через smtp.yandex.ru:465.

Здесь — постановка письма в очередь и контракт для релея:
list_pending / mark_sent / mark_error.
"""
import logging
from datetime import timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import EMAIL_OUTBOX_STATUS, EmailOutbox

logger = logging.getLogger("dz_fastapi")

MAX_SEND_ATTEMPTS = 5
# Аренда захвата: если воркер «умер», не отметив письмо, через это время
# письмо снова доступно для захвата другим воркером.
DEFAULT_CLAIM_LEASE_SECONDS = 300


async def enqueue_email(
    session: AsyncSession,
    *,
    to_email: str,
    from_email: Optional[str] = None,
    subject: Optional[str] = None,
    body_text: Optional[str] = None,
    body_html: Optional[str] = None,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    reply_to: Optional[str] = None,
    attachments: Optional[list[dict[str, Any]]] = None,
    source_type: Optional[str] = None,
    source_id: Optional[int] = None,
    commit: bool = True,
) -> EmailOutbox:
    if not to_email:
        raise ValueError("Не задан адрес получателя")
    row = EmailOutbox(
        status=EMAIL_OUTBOX_STATUS.PENDING,
        to_email=to_email,
        from_email=from_email,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        in_reply_to=in_reply_to,
        references=references,
        reply_to=reply_to,
        attachments=attachments or [],
        source_type=source_type,
        source_id=source_id,
        attempts=0,
    )
    session.add(row)
    if commit:
        await session.commit()
        await session.refresh(row)
    else:
        await session.flush()
    return row


async def list_pending_outbox(
    session: AsyncSession, *, limit: int = 50
) -> list[EmailOutbox]:
    rows = (
        await session.execute(
            select(EmailOutbox)
            .where(EmailOutbox.status == EMAIL_OUTBOX_STATUS.PENDING)
            .order_by(EmailOutbox.id.asc())
            .limit(max(1, min(int(limit or 50), 200)))
        )
    ).scalars().all()
    return list(rows)


async def list_outbox_for_source(
    session: AsyncSession, *, source_type: str, source_id: int
) -> list[EmailOutbox]:
    rows = (
        await session.execute(
            select(EmailOutbox)
            .where(
                EmailOutbox.source_type == source_type,
                EmailOutbox.source_id == int(source_id),
            )
            .order_by(EmailOutbox.id.desc())
        )
    ).scalars().all()
    return list(rows)


async def claim_pending_outbox(
    session: AsyncSession,
    *,
    worker: str,
    limit: int = 25,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
) -> list[EmailOutbox]:
    """Атомарно «захватывает» pending-письма за воркером, чтобы несколько
    релеев не отправили одно письмо дважды. Берёт письма, у которых нет
    активного захвата (claimed_at пуст или аренда истекла), помечает их
    claimed_by/claimed_at и возвращает. Использует SKIP LOCKED."""
    now = now_moscow()
    cutoff = now - timedelta(seconds=max(30, int(lease_seconds)))
    limit = max(1, min(int(limit or 25), 200))
    locked = (
        await session.execute(
            select(EmailOutbox.id)
            .where(
                EmailOutbox.status == EMAIL_OUTBOX_STATUS.PENDING,
                or_(
                    EmailOutbox.claimed_at.is_(None),
                    EmailOutbox.claimed_at < cutoff,
                ),
            )
            .order_by(EmailOutbox.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()
    if not locked:
        return []
    rows = (
        await session.execute(
            select(EmailOutbox).where(EmailOutbox.id.in_(locked))
        )
    ).scalars().all()
    for row in rows:
        row.claimed_by = (worker or "relay")[:128]
        row.claimed_at = now
        session.add(row)
    await session.commit()
    for row in rows:
        await session.refresh(row)
    return list(rows)


async def mark_outbox_sent(
    session: AsyncSession, *, outbox_id: int
) -> EmailOutbox:
    row = await session.get(EmailOutbox, outbox_id)
    if row is None:
        raise ValueError("Письмо не найдено")
    row.status = EMAIL_OUTBOX_STATUS.SENT
    row.sent_at = now_moscow()
    row.attempts = int(row.attempts or 0) + 1
    row.last_error = None
    row.claimed_by = None
    row.claimed_at = None
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def mark_outbox_error(
    session: AsyncSession,
    *,
    outbox_id: int,
    error: str,
    retry: bool = True,
) -> EmailOutbox:
    row = await session.get(EmailOutbox, outbox_id)
    if row is None:
        raise ValueError("Письмо не найдено")
    row.attempts = int(row.attempts or 0) + 1
    row.last_error = (error or "")[:2000]
    # Снимаем захват: письмо снова свободно для повторного взятия релеем
    row.claimed_by = None
    row.claimed_at = None
    # Пока не исчерпаны попытки и разрешён ретрай — оставляем pending
    if retry and row.attempts < MAX_SEND_ATTEMPTS:
        row.status = EMAIL_OUTBOX_STATUS.PENDING
    else:
        row.status = EMAIL_OUTBOX_STATUS.ERROR
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


async def cancel_outbox(
    session: AsyncSession, *, outbox_id: int
) -> EmailOutbox:
    row = await session.get(EmailOutbox, outbox_id)
    if row is None:
        raise ValueError("Письмо не найдено")
    if row.status == EMAIL_OUTBOX_STATUS.SENT:
        raise ValueError("Письмо уже отправлено")
    row.status = EMAIL_OUTBOX_STATUS.CANCELLED
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row
