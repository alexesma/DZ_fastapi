"""Повторы неушедших писем и контроль журнала исходящей почты."""

import asyncio
import logging
import os
from datetime import timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.email_account import EmailAccount
from dz_fastapi.models.email_send_log import EmailSendLog
from dz_fastapi.services.email import (
    _send_email_with_attachment_impl,
    build_email_delivery_kwargs,
    run_send_with_error_capture,
    send_via_resend_fallback,
)
from dz_fastapi.services.email_journal import (
    EMAIL_MAX_ATTEMPTS,
    EMAIL_RETRY_DELAYS_MIN,
    is_permanent_smtp_error,
)
from dz_fastapi.services.notifications import create_admin_notifications

logger = logging.getLogger("dz_fastapi")

KEEP_SENT_DAYS = 60
MAX_EVENTS = 30
BATCH_LIMIT = 20


def _append_event(row: EmailSendLog, ok: bool, error: str | None) -> None:
    events = list(row.events or [])
    events.append({"at": now_moscow().isoformat(), "ok": ok, "error": None if ok else error})
    row.events = events[-MAX_EVENTS:]


def _remove_attachment(row: EmailSendLog) -> None:
    path = row.attachment_path
    row.attachment_path = None
    if path:
        try:
            os.remove(path)
        except OSError:
            pass


async def _resend(session: AsyncSession, row: EmailSendLog) -> tuple[bool, str | None]:
    kwargs: dict = {}
    if row.from_email:
        account = (
            await session.execute(
                select(EmailAccount).where(EmailAccount.email == row.from_email)
            )
        ).scalar_one_or_none()
        if account is not None:
            kwargs = build_email_delivery_kwargs(account)
    attachment = None
    if row.attachment_path:
        try:
            with open(row.attachment_path, "rb") as handle:
                attachment = handle.read()
        except OSError as exc:
            return False, f"Не найдено вложение для повтора: {exc}"

    def send():
        return _send_email_with_attachment_impl(
            row.to_email,
            row.subject,
            row.body or "",
            attachment,
            row.attachment_name,
            bool(row.is_html),
            **kwargs,
        )

    return await asyncio.to_thread(run_send_with_error_capture, send)


async def _resend_via_fallback(row: EmailSendLog) -> tuple[bool, str | None]:
    attachment = None
    if row.attachment_path:
        try:
            with open(row.attachment_path, "rb") as handle:
                attachment = handle.read()
        except OSError as exc:
            return False, f"Resend: не найдено вложение: {exc}"
    return await asyncio.to_thread(
        send_via_resend_fallback,
        row.to_email,
        row.subject,
        row.body or "",
        attachment,
        row.attachment_name,
        bool(row.is_html),
    )


async def retry_due_emails(session: AsyncSession) -> dict[str, int]:
    """Повторяет письма, у которых подошло время; при исчерпании — тревога."""
    now = now_moscow()
    rows = (
        await session.execute(
            select(EmailSendLog)
            .where(
                EmailSendLog.status == "retry",
                EmailSendLog.next_retry_at <= now,
            )
            .order_by(EmailSendLog.next_retry_at.asc())
            .limit(BATCH_LIMIT)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()
    result = {"retried": 0, "sent": 0, "failed": 0}
    for row in rows:
        if is_permanent_smtp_error(row.last_error):
            # SMTP отказал окончательно (5xx) — повтор тем же каналом бессмыслен.
            ok, error = False, row.last_error
        else:
            ok, error = await _resend(session, row)
        result["retried"] += 1
        row.attempts = int(row.attempts or 0) + 1
        _append_event(row, ok, error)
        if ok:
            row.status = "sent"
            row.sent_at = now_moscow()
            row.next_retry_at = None
            row.last_error = None
            row.body = None
            _remove_attachment(row)
            result["sent"] += 1
            continue
        row.last_error = (error or "")[:2000] or None
        if row.attempts >= EMAIL_MAX_ATTEMPTS or is_permanent_smtp_error(error):
            fb_ok, fb_error = await _resend_via_fallback(row)
            _append_event(row, fb_ok, fb_error or "отправлено через Resend (запасной канал)")
            if fb_ok:
                row.status = "sent"
                row.sent_at = now_moscow()
                row.next_retry_at = None
                row.last_error = None
                row.body = None
                _remove_attachment(row)
                result["sent"] += 1
                await session.commit()
                continue
            row.last_error = f"{row.last_error or ''} | {fb_error}"[:2000]
            row.status = "failed"
            row.next_retry_at = None
            result["failed"] += 1
            await session.commit()
            await _alert_failed(session, row)
            continue
        delay = EMAIL_RETRY_DELAYS_MIN[min(row.attempts - 1, len(EMAIL_RETRY_DELAYS_MIN) - 1)]
        row.next_retry_at = now_moscow() + timedelta(minutes=delay)
    await session.commit()
    return result


async def _alert_failed(session: AsyncSession, row: EmailSendLog) -> None:
    try:
        await create_admin_notifications(
            session=session,
            title="Письмо не отправлено",
            message=(
                f"Кому: {row.to_email}. Тема: {row.subject or '—'}. "
                f"Попыток: {row.attempts}. Ошибка: {(row.last_error or '')[:300]}"
            ),
            level="error",
            link="/admin/email-log",
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Could not create email failure notification: %s", exc, exc_info=True)


async def cleanup_email_log(session: AsyncSession) -> int:
    """Удаляет старые записи об ушедших письмах (неушедшие храним)."""
    cutoff = now_moscow() - timedelta(days=KEEP_SENT_DAYS)
    deleted = await session.execute(
        delete(EmailSendLog).where(
            EmailSendLog.status == "sent", EmailSendLog.created_at < cutoff
        )
    )
    await session.commit()
    return int(deleted.rowcount or 0)


async def schedule_manual_retry(session: AsyncSession, row: EmailSendLog) -> None:
    """Ставит письмо из журнала на немедленный повтор (ещё одна попытка)."""
    if not row.retry_allowed or (not row.body and not row.attachment_path):
        raise ValueError("Это письмо нельзя повторить из журнала")
    if row.status == "sent":
        raise ValueError("Письмо уже отправлено")
    row.status = "retry"
    row.attempts = min(int(row.attempts or 0), EMAIL_MAX_ATTEMPTS - 1)
    row.next_retry_at = now_moscow()
    await session.commit()
