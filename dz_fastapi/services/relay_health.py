"""Heartbeat and health checks for the external email relay."""

import os
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotification, AppNotificationLevel
from dz_fastapi.models.partner import EMAIL_OUTBOX_STATUS, EmailOutbox
from dz_fastapi.models.relay import RelayHeartbeat
from dz_fastapi.services.notifications import create_admin_notifications

RELAY_OFFLINE_NOTIFICATION_TYPE = "email_relay_offline_action"
RELAY_HEARTBEAT_TIMEOUT_SECONDS = max(
    60,
    int(os.getenv("EMAIL_RELAY_HEARTBEAT_TIMEOUT_SECONDS", "180")),
)


async def record_relay_heartbeat(
    session: AsyncSession,
    *,
    worker: str,
) -> None:
    worker_id = str(worker or "relay").strip()[:128] or "relay"
    seen_at = now_moscow()
    statement = insert(RelayHeartbeat).values(
        worker_id=worker_id,
        last_seen_at=seen_at,
        created_at=seen_at,
    )
    statement = statement.on_conflict_do_update(
        index_elements=[RelayHeartbeat.worker_id],
        set_={"last_seen_at": seen_at},
    )
    await session.execute(statement)


def _is_relay_notification(item: AppNotification) -> bool:
    payload = item.payload if isinstance(item.payload, dict) else {}
    return payload.get("notification_type") == RELAY_OFFLINE_NOTIFICATION_TYPE


async def check_email_relay_health(session: AsyncSession) -> dict[str, object]:
    now = now_moscow()
    timeout = timedelta(seconds=RELAY_HEARTBEAT_TIMEOUT_SECONDS)
    pending_count, oldest_pending_at = (
        await session.execute(
            select(func.count(EmailOutbox.id), func.min(EmailOutbox.created_at)).where(
                EmailOutbox.status == EMAIL_OUTBOX_STATUS.PENDING
            )
        )
    ).one()
    pending_count = int(pending_count or 0)
    last_seen_at = await session.scalar(select(func.max(RelayHeartbeat.last_seen_at)))
    open_notifications = list(
        (
            await session.scalars(
                select(AppNotification).where(AppNotification.read_at.is_(None))
            )
        ).all()
    )
    relay_notifications = [item for item in open_notifications if _is_relay_notification(item)]

    heartbeat_stale = last_seen_at is None or last_seen_at < now - timeout
    pending_is_old = oldest_pending_at is not None and oldest_pending_at < now - timeout
    relay_offline = heartbeat_stale and (
        last_seen_at is not None
        or (pending_count > 0 and pending_is_old)
    )

    if not relay_offline:
        for item in relay_notifications:
            item.read_at = now
            session.add(item)
        await session.commit()
        return {
            "status": "online" if last_seen_at and not heartbeat_stale else "idle",
            "pending_count": pending_count,
            "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
            "notifications_closed": len(relay_notifications),
        }

    if not relay_notifications:
        await create_admin_notifications(
            session=session,
            title="Не работает почтовый релей",
            message=(
                f"Программа DZ Email Relay не забирает очередь более "
                f"{RELAY_HEARTBEAT_TIMEOUT_SECONDS // 60} мин. "
                f"Ожидают отправки писем: {pending_count}. "
                "Проверьте программу релея на рабочем компьютере."
            ),
            level=AppNotificationLevel.ERROR,
            link="/admin/monitor",
            payload={
                "notification_type": RELAY_OFFLINE_NOTIFICATION_TYPE,
                "pending_count": pending_count,
                "oldest_pending_at": oldest_pending_at.isoformat(),
                "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
                "timeout_seconds": RELAY_HEARTBEAT_TIMEOUT_SECONDS,
            },
            commit=False,
        )
        await session.commit()

    return {
        "status": "offline",
        "pending_count": pending_count,
        "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
        "notifications_open": max(len(relay_notifications), 1),
    }
