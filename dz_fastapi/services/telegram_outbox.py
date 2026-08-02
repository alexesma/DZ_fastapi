"""Persistent Telegram queue consumed by the external HTTPS relay."""

from __future__ import annotations

import asyncio
import base64
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.constants import get_upload_dir
from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import TELEGRAM_OUTBOX_STATUS, TelegramOutbox

MAX_SEND_ATTEMPTS = 5
DEFAULT_CLAIM_LEASE_SECONDS = 300
MAX_TELEGRAM_DOCUMENT_BYTES = max(
    1,
    int(os.getenv("TELEGRAM_RELAY_MAX_DOCUMENT_MB", "25")),
) * 1024 * 1024
TELEGRAM_OUTBOX_DIR = Path(get_upload_dir()) / "telegram_outbox"


def _safe_filename(value: str) -> str:
    name = Path(str(value or "document.bin")).name
    sanitized = re.sub(r"[^A-Za-z0-9._-]", "_", name).strip("._")
    return sanitized or "document.bin"


async def enqueue_telegram_message(
    session: AsyncSession,
    *,
    chat_id: str,
    text: str,
    parse_mode: str | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
    commit: bool = True,
) -> TelegramOutbox:
    if not str(chat_id or "").strip():
        raise ValueError("Не задан Telegram chat_id")
    if not str(text or "").strip():
        raise ValueError("Не задан текст Telegram-сообщения")
    row = TelegramOutbox(
        status=TELEGRAM_OUTBOX_STATUS.PENDING,
        chat_id=str(chat_id).strip(),
        text=str(text),
        parse_mode=(str(parse_mode).strip() or None) if parse_mode else None,
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


async def enqueue_telegram_document(
    session: AsyncSession,
    *,
    chat_id: str,
    file_bytes: bytes,
    file_name: str,
    caption: str = "",
    content_type: str | None = None,
    source_type: str | None = None,
    source_id: int | None = None,
    commit: bool = True,
) -> TelegramOutbox:
    if not str(chat_id or "").strip():
        raise ValueError("Не задан Telegram chat_id")
    payload = bytes(file_bytes or b"")
    if not payload:
        raise ValueError("Telegram-документ пуст")
    if len(payload) > MAX_TELEGRAM_DOCUMENT_BYTES:
        raise ValueError("Telegram-документ превышает допустимый размер")

    safe_name = _safe_filename(file_name)
    TELEGRAM_OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    path = TELEGRAM_OUTBOX_DIR / f"{uuid4().hex}_{safe_name}"
    await asyncio.to_thread(path.write_bytes, payload)
    row = TelegramOutbox(
        status=TELEGRAM_OUTBOX_STATUS.PENDING,
        chat_id=str(chat_id).strip(),
        document_name=str(file_name or safe_name),
        document_path=str(path),
        document_content_type=(
            content_type or "application/octet-stream"
        ),
        caption=str(caption or "") or None,
        source_type=source_type,
        source_id=source_id,
        attempts=0,
    )
    session.add(row)
    try:
        if commit:
            await session.commit()
            await session.refresh(row)
        else:
            await session.flush()
    except Exception:
        try:
            await asyncio.to_thread(path.unlink)
        except OSError:
            pass
        raise
    return row


async def list_pending_telegram_outbox(
    session: AsyncSession,
    *,
    limit: int = 50,
) -> list[TelegramOutbox]:
    rows = (
        await session.execute(
            select(TelegramOutbox)
            .where(TelegramOutbox.status == TELEGRAM_OUTBOX_STATUS.PENDING)
            .order_by(TelegramOutbox.id.asc())
            .limit(max(1, min(int(limit or 50), 200)))
        )
    ).scalars().all()
    return list(rows)


async def claim_pending_telegram_outbox(
    session: AsyncSession,
    *,
    worker: str,
    limit: int = 25,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
) -> list[TelegramOutbox]:
    now = now_moscow()
    cutoff = now - timedelta(seconds=max(30, int(lease_seconds)))
    limit = max(1, min(int(limit or 25), 200))
    locked_ids = (
        await session.execute(
            select(TelegramOutbox.id)
            .where(
                TelegramOutbox.status == TELEGRAM_OUTBOX_STATUS.PENDING,
                or_(
                    TelegramOutbox.claimed_at.is_(None),
                    TelegramOutbox.claimed_at < cutoff,
                ),
            )
            .order_by(TelegramOutbox.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    ).scalars().all()
    if not locked_ids:
        return []
    rows = (
        await session.execute(
            select(TelegramOutbox).where(
                TelegramOutbox.id.in_(locked_ids)
            )
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


def serialize_telegram_outbox_for_relay(
    row: TelegramOutbox,
) -> dict[str, Any]:
    document_base64 = None
    document_error = None
    if row.document_path:
        path = Path(row.document_path).resolve()
        root = TELEGRAM_OUTBOX_DIR.resolve()
        try:
            inside_root = os.path.commonpath((str(path), str(root))) == str(
                root
            )
        except ValueError:
            inside_root = False
        if not inside_root:
            document_error = "Недопустимый путь Telegram-документа"
        else:
            try:
                payload = path.read_bytes()
                if len(payload) > MAX_TELEGRAM_DOCUMENT_BYTES:
                    document_error = "Telegram-документ слишком большой"
                else:
                    document_base64 = base64.b64encode(payload).decode("ascii")
            except OSError as exc:
                document_error = str(exc)
    return {
        "id": row.id,
        "status": str(getattr(row.status, "value", row.status)),
        "chat_id": row.chat_id,
        "text": row.text,
        "parse_mode": row.parse_mode,
        "document_name": row.document_name,
        "document_content_type": row.document_content_type,
        "document_base64": document_base64,
        "document_error": document_error,
        "caption": row.caption,
        "source_type": row.source_type,
        "source_id": row.source_id,
        "attempts": int(row.attempts or 0),
        "last_error": row.last_error,
        "sent_at": row.sent_at,
        "created_at": row.created_at,
    }


async def mark_telegram_outbox_sent(
    session: AsyncSession,
    *,
    outbox_id: int,
) -> TelegramOutbox:
    row = await session.get(TelegramOutbox, outbox_id)
    if row is None:
        raise ValueError("Telegram-сообщение не найдено")
    row.status = TELEGRAM_OUTBOX_STATUS.SENT
    row.sent_at = now_moscow()
    row.attempts = int(row.attempts or 0) + 1
    row.last_error = None
    row.claimed_by = None
    row.claimed_at = None
    document_path = row.document_path
    row.document_path = None
    session.add(row)
    await session.commit()
    await session.refresh(row)
    if document_path:
        try:
            await asyncio.to_thread(Path(document_path).unlink)
        except FileNotFoundError:
            pass
        except OSError:
            pass
    return row


async def mark_telegram_outbox_error(
    session: AsyncSession,
    *,
    outbox_id: int,
    error: str,
    retry: bool = True,
) -> TelegramOutbox:
    row = await session.get(TelegramOutbox, outbox_id)
    if row is None:
        raise ValueError("Telegram-сообщение не найдено")
    if row.status == TELEGRAM_OUTBOX_STATUS.SENT:
        return row
    row.attempts = int(row.attempts or 0) + 1
    row.last_error = str(error or "")[:2000]
    row.claimed_by = None
    row.claimed_at = None
    document_path = None
    if retry and row.attempts < MAX_SEND_ATTEMPTS:
        row.status = TELEGRAM_OUTBOX_STATUS.PENDING
    else:
        row.status = TELEGRAM_OUTBOX_STATUS.ERROR
        document_path = row.document_path
        row.document_path = None
    session.add(row)
    await session.commit()
    await session.refresh(row)
    if document_path:
        try:
            await asyncio.to_thread(Path(document_path).unlink)
        except FileNotFoundError:
            pass
        except OSError:
            pass
    return row
