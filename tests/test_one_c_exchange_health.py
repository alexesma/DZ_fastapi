from datetime import timedelta

import pytest
from sqlalchemy import func, select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.one_c import OneCEnterpriseConfig, OneCExchangeEvent, OneCExchangeEventStatus
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.services import one_c_exchange_health as health_module
from dz_fastapi.services.one_c_exchange_health import exchange_health, notify_exchange_stall


def _transport(pending_minutes=None, incoming_minutes=None):
    now = now_moscow()
    return {
        "pending_body": (
            {
                "message_no": 12,
                "sent_at": (now - timedelta(minutes=pending_minutes)).isoformat(),
            }
            if pending_minutes is not None
            else None
        ),
        "incoming_messages": (
            [{"modified_at": (now - timedelta(minutes=incoming_minutes)).isoformat()}]
            if incoming_minutes is not None
            else []
        ),
        "last_message_at": None,
    }


async def _enable(session, enabled=True):
    session.add(OneCEnterpriseConfig(id=1, enabled=enabled))
    await session.flush()


def _event(key, *, created_minutes_ago=0, error=None):
    return OneCExchangeEvent(
        entity_type="shipment",
        entity_id=key,
        event_type="posted",
        payload={"document_id": key},
        idempotency_key=f"health-test:{key}",
        status=OneCExchangeEventStatus.PENDING,
        ed_last_error=error,
        created_at=now_moscow() - timedelta(minutes=created_minutes_ago),
    )


@pytest.mark.asyncio
async def test_exchange_is_healthy_while_unf_keeps_reading(test_session):
    await _enable(test_session)
    test_session.add(_event(1, created_minutes_ago=2))
    await test_session.flush()
    health = await exchange_health(test_session, _transport(pending_minutes=4))
    assert health["stalled"] is False
    assert health["waiting_events"] == 1


@pytest.mark.asyncio
async def test_unread_message_and_stuck_queue_are_reported(test_session):
    await _enable(test_session)
    test_session.add_all(
        [_event(1, created_minutes_ago=90), _event(2, error="нет договора")]
    )
    await test_session.flush()

    unread = await exchange_health(test_session, _transport(pending_minutes=45))
    assert unread["stalled"] is True
    assert "№ 12" in unread["reasons"][0]
    assert (unread["waiting_events"], unread["error_events"]) == (1, 1)

    stuck = await exchange_health(test_session, _transport())
    assert stuck["stalled"] is True
    assert "ждут отправки" in stuck["reasons"][0]

    incoming = await exchange_health(test_session, _transport(incoming_minutes=50))
    assert "лежит неразобранным" in incoming["reasons"][0]


@pytest.mark.asyncio
async def test_disabled_exchange_is_never_stalled(test_session):
    await _enable(test_session, enabled=False)
    test_session.add(_event(1, created_minutes_ago=500))
    await test_session.flush()
    health = await exchange_health(test_session, _transport(pending_minutes=500))
    assert health["stalled"] is False


@pytest.mark.asyncio
async def test_stall_notification_is_not_repeated(test_session, monkeypatch):
    monkeypatch.setattr(health_module, "WORK_HOURS", range(0, 24))
    test_session.add(
        User(
            name="Admin",
            email="stall-admin@example.com",
            password_hash="unused",
            role=UserRole.ADMIN,
            status=UserStatus.ACTIVE,
        )
    )
    await _enable(test_session)
    await test_session.commit()

    first = await notify_exchange_stall(test_session, _transport(pending_minutes=45))
    assert first["notified"] is True
    second = await notify_exchange_stall(test_session, _transport(pending_minutes=50))
    assert second["notified"] is False
    assert await test_session.scalar(select(func.count(AppNotification.id))) == 1

    # Вне рабочего времени не будим.
    monkeypatch.setattr(health_module, "WORK_HOURS", range(0, 0))
    await test_session.execute(AppNotification.__table__.delete())
    night = await notify_exchange_stall(test_session, _transport(pending_minutes=45))
    assert night["stalled"] is True and night["notified"] is False
