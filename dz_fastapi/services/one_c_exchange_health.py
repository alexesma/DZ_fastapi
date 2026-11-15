"""Контроль простоя обмена с УНФ.

Обмен идёт через FTP: мы кладём сообщение, УНФ читает его по расписанию и
отвечает. Если УНФ перестала читать (выключена, сломан сценарий, недоступен
FTP), документы молча копятся. Здесь определяется, что обмен стоит дольше
порога, и администраторам уходит оповещение.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotification, AppNotificationLevel
from dz_fastapi.models.one_c import OneCEnterpriseConfig, OneCExchangeEvent
from dz_fastapi.services.notifications import create_admin_notifications
from dz_fastapi.services.one_c_enterprise_data import get_exchange_status

STALL_TITLE = "Обмен с 1С стоит"
STALL_LINK = "/documents/1c"
# УНФ читает FTP каждые 3–5 минут, поэтому полчаса без движения — уже сбой.
STALL_MINUTES = max(5, int(os.getenv("ONE_C_ED_STALL_MINUTES", "30")))
REMIND_EVERY = timedelta(hours=3)
WORK_HOURS = range(8, 20)


def _parse(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=now_moscow().tzinfo)


def _minutes(since: Optional[datetime], now: datetime) -> Optional[int]:
    if since is None:
        return None
    return max(0, int((now - since).total_seconds() // 60))


async def exchange_health(
    session: AsyncSession,
    transport: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Состояние обмена: стоит ли он, почему и сколько документов ждёт."""
    if transport is None:
        transport = await asyncio.to_thread(get_exchange_status)
    config = await session.get(OneCEnterpriseConfig, 1)
    enabled = bool(config and config.enabled)
    now = now_moscow()

    waiting_filter = (
        OneCExchangeEvent.id >= ((config.first_event_id if config else None) or 1),
        OneCExchangeEvent.ed_message_no.is_(None),
        OneCExchangeEvent.ed_last_error.is_(None),
        OneCExchangeEvent.ed_excluded_at.is_(None),
        OneCExchangeEvent.event_type.in_(("posted", "cancelled")),
    )
    waiting, oldest_waiting = (
        await session.execute(
            select(func.count(OneCExchangeEvent.id), func.min(OneCExchangeEvent.created_at)).where(
                *waiting_filter
            )
        )
    ).one()
    errors = await session.scalar(
        select(func.count(OneCExchangeEvent.id)).where(
            OneCExchangeEvent.ed_last_error.is_not(None),
            OneCExchangeEvent.ed_excluded_at.is_(None),
        )
    )

    pending = transport.get("pending_body") or None
    pending_minutes = _minutes(_parse((pending or {}).get("sent_at")), now)
    incoming_minutes = None
    for item in transport.get("incoming_messages") or []:
        age = _minutes(_parse(item.get("modified_at")), now)
        if age is not None:
            incoming_minutes = max(incoming_minutes or 0, age)
    waiting_minutes = _minutes(oldest_waiting, now)

    reasons: list[str] = []
    if enabled:
        if pending_minutes is not None and pending_minutes >= STALL_MINUTES:
            reasons.append(
                f"УНФ {pending_minutes} мин. не читает сообщение "
                f"№ {pending.get('message_no')} с документами"
            )
        if incoming_minutes is not None and incoming_minutes >= STALL_MINUTES:
            reasons.append(
                f"Входящее сообщение УНФ {incoming_minutes} мин. лежит неразобранным"
            )
        if (
            not reasons
            and waiting
            and waiting_minutes is not None
            and waiting_minutes >= STALL_MINUTES
        ):
            reasons.append(
                f"Документы ждут отправки {waiting_minutes} мин., а обмен не движется"
            )
    return {
        "enabled": enabled,
        "stalled": bool(reasons),
        "reasons": reasons,
        "stall_minutes": STALL_MINUTES,
        "waiting_events": int(waiting or 0),
        "waiting_minutes": waiting_minutes,
        "error_events": int(errors or 0),
        "pending_message_no": (pending or {}).get("message_no"),
        "pending_minutes": pending_minutes,
        "incoming_minutes": incoming_minutes,
        "last_message_at": transport.get("last_message_at"),
    }


async def notify_exchange_stall(
    session: AsyncSession,
    transport: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Оповещает администраторов о простое — в рабочее время и не чаще раза в 3 часа."""
    health = await exchange_health(session, transport)
    health["notified"] = False
    now = now_moscow()
    if not health["stalled"] or now.hour not in WORK_HOURS:
        return health
    recent = await session.scalar(
        select(func.count(AppNotification.id)).where(
            AppNotification.title == STALL_TITLE,
            AppNotification.created_at >= now - REMIND_EVERY,
        )
    )
    if recent:
        return health
    details = "; ".join(health["reasons"])
    extra = []
    if health["waiting_events"]:
        extra.append(f"ждут отправки: {health['waiting_events']}")
    if health["error_events"]:
        extra.append(f"с ошибкой подготовки: {health['error_events']}")
    await create_admin_notifications(
        session,
        title=STALL_TITLE,
        message=details + (". Документов " + ", ".join(extra) if extra else "") + ".",
        level=AppNotificationLevel.ERROR,
        link=STALL_LINK,
    )
    health["notified"] = True
    return health
