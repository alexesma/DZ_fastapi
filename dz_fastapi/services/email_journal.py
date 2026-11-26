"""Запись журнала исходящих писем из синхронного транспортного слоя.

Письма отправляются синхронно (в потоке), поэтому журнал пишем обычным
синхронным подключением. Любая ошибка журнала только логируется: письмо
из-за неё не должно теряться или падать.
"""

import logging
import os
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import create_engine, insert
from sqlalchemy.pool import NullPool

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.email_send_log import EmailSendLog

logger = logging.getLogger("dz_fastapi")

# Паузы между повторами, минуты: после 1-й, 2-й, 3-й, 4-й неудачи.
EMAIL_RETRY_DELAYS_MIN = (1, 2, 3, 5)
EMAIL_MAX_ATTEMPTS = len(EMAIL_RETRY_DELAYS_MIN) + 1
RETRY_DIR = os.getenv("EMAIL_RETRY_DIR", "uploads/email_retry")

_engine = None
_engine_lock = threading.Lock()


def journal_enabled() -> bool:
    if os.getenv("EMAIL_SEND_LOG_ENABLED", "1") == "0":
        return False
    if os.getenv("PYTEST_CURRENT_TEST") and os.getenv("EMAIL_LOG_IN_TESTS") != "1":
        return False
    return True


def _get_engine():
    global _engine
    with _engine_lock:
        if _engine is None:
            from dz_fastapi.core.config import settings

            url = settings.get_database_url(test=bool(os.getenv("PYTEST_CURRENT_TEST")))
            url = url.replace("+asyncpg", "+psycopg2").replace(
                "postgresql://", "postgresql+psycopg2://"
            )
            _engine = create_engine(url, poolclass=NullPool, pool_pre_ping=True)
        return _engine


def _recipients_text(to_email: Any) -> str:
    if isinstance(to_email, (list, tuple, set)):
        return ", ".join(str(item) for item in to_email)
    return str(to_email or "")


def _store_attachment(data: Optional[bytes], name: Optional[str]) -> Optional[str]:
    if not data:
        return None
    directory = Path(RETRY_DIR) / now_moscow().strftime("%Y%m%d")
    directory.mkdir(parents=True, exist_ok=True)
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (name or "file"))
    path = directory / f"{now_moscow().strftime('%H%M%S%f')}_{safe}"
    path.write_bytes(data)
    return str(path)


def record_send(
    *,
    to_email: Any,
    from_email: Optional[str],
    subject: Optional[str],
    transport: Optional[str],
    source: Optional[str],
    source_id: Optional[int],
    attachment_bytes: Optional[bytes],
    attachment_name: Optional[str],
    body: Optional[str],
    is_html: bool,
    ok: bool,
    error: Optional[str],
    duration_ms: int,
    auto_retry: bool,
) -> Optional[int]:
    """Пишет результат первой попытки. Возвращает id строки журнала."""
    if not journal_enabled():
        return None
    try:
        now = now_moscow()
        event = {"at": now.isoformat(), "ok": ok, "error": None if ok else error}
        values = {
            "created_at": now,
            "updated_at": now,
            "to_email": _recipients_text(to_email)[:1000],
            "from_email": (from_email or "")[:255] or None,
            "subject": (subject or "")[:998] or None,
            "transport": transport,
            "source": source,
            "source_id": source_id,
            "attachment_name": attachment_name,
            "attachment_size": len(attachment_bytes) if attachment_bytes else None,
            "attempts": 1,
            "events": [event],
            "is_html": bool(is_html),
            "duration_ms": duration_ms,
            "auto_retry": bool(auto_retry),
        }
        if ok:
            values.update(status="sent", sent_at=now, retry_allowed=False)
        else:
            values.update(
                status="retry" if auto_retry else "failed",
                next_retry_at=(
                    now + timedelta(minutes=EMAIL_RETRY_DELAYS_MIN[0]) if auto_retry else None
                ),
                last_error=(error or "")[:2000] or None,
                body=body,
                attachment_path=_store_attachment(attachment_bytes, attachment_name),
                # Письма, чей процесс ведёт свой повтор (заказы поставщикам),
                # из журнала не повторяем: статус заказа знает только он.
                retry_allowed=(source != "supplier_order"),
            )
        with _get_engine().begin() as connection:
            result = connection.execute(
                insert(EmailSendLog).values(**values).returning(EmailSendLog.id)
            )
            return result.scalar_one()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not write email send log: %s", exc)
        return None
