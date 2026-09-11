from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.api.notifications import handle_pricelist_stale_action
from dz_fastapi.core.time import now_moscow
from dz_fastapi.main import app
from dz_fastapi.models.notification import AppNotification
from dz_fastapi.models.partner import PriceList, ProviderPricelistReview
from dz_fastapi.models.user import User, UserRole, UserStatus
from dz_fastapi.schemas.notification import PricelistStaleActionRequest
from dz_fastapi.services.notifications import notify_admin_all
from dz_fastapi.services.scheduler import check_provider_pricelist_staleness_task


@pytest.mark.asyncio
async def test_notify_admin_all_uses_relay_aware_telegram_service(monkeypatch):
    telegram_messages = []

    async def fake_create_admin_notifications(*_args, **_kwargs):
        return [SimpleNamespace(id=1)]

    async def fake_send_message(text):
        telegram_messages.append(text)

    monkeypatch.setattr(
        "dz_fastapi.services.notifications.create_admin_notifications",
        fake_create_admin_notifications,
    )
    monkeypatch.setattr(
        "dz_fastapi.services.telegram.send_message_to_telegram",
        fake_send_message,
    )

    notifications = await notify_admin_all(
        SimpleNamespace(),
        title="Проверка",
        message="Сбой задания",
        level="error",
        link="/admin/settings",
    )

    assert len(notifications) == 1
    assert telegram_messages == [
        "🔴 Проверка\n\nСбой задания\n\n/admin/settings"
    ]


@pytest.mark.asyncio
async def test_stale_pricelist_action_can_snooze_and_extend(
    test_session,
    created_pricelist_config,
):
    admin = User(
        name="Admin",
        email="stale-admin@example.com",
        password_hash="test",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    test_session.add(admin)
    await test_session.flush()
    payload = {
        "notification_type": "pricelist_stale_action",
        "provider_config_id": created_pricelist_config.id,
    }
    first = AppNotification(
        user_id=admin.id,
        title="Устарел прайс",
        message="Нужно решение",
        level="warning",
        payload=payload,
    )
    test_session.add(first)
    await test_session.commit()

    snoozed = await handle_pricelist_stale_action(
        notification_id=first.id,
        request=PricelistStaleActionRequest(action="snooze_30_minutes"),
        session=test_session,
        current_user=admin,
    )
    assert snoozed.available_at >= now_moscow() + timedelta(minutes=29)
    assert (await test_session.get(AppNotification, first.id)).read_at is not None

    replacement = await test_session.get(AppNotification, snoozed.notification_id)
    extended = await handle_pricelist_stale_action(
        notification_id=replacement.id,
        request=PricelistStaleActionRequest(action="extend_one_day"),
        session=test_session,
        current_user=admin,
    )
    assert extended.override_until >= now_moscow() + timedelta(hours=23)
    await test_session.refresh(created_pricelist_config)
    assert created_pricelist_config.stale_override_until == extended.override_until


@pytest.mark.asyncio
async def test_stale_check_creates_actionable_admin_notification(
    test_session,
    created_providers,
    created_pricelist_config,
):
    admin = User(
        name="Alert Admin",
        email="stale-check-admin@example.com",
        password_hash="test",
        role=UserRole.ADMIN,
        status=UserStatus.ACTIVE,
    )
    created_pricelist_config.max_days_without_update = 1
    test_session.add_all(
        [
            admin,
            created_pricelist_config,
            PriceList(
                provider_id=created_providers[0].id,
                provider_config_id=created_pricelist_config.id,
                date=now_moscow().date() - timedelta(days=5),
                is_active=True,
            ),
        ]
    )
    await test_session.commit()

    review = ProviderPricelistReview(
        provider_id=created_providers[0].id,
        provider_config_id=created_pricelist_config.id,
        source_filename="waiting.xlsx",
        file_path="uploads/pricelist_reviews/waiting.xlsx",
        file_extension="xlsx",
        file_sha256="b" * 64,
        status="pending",
        reasons=["Резкое изменение состава"],
        metrics={},
        examples=[],
    )
    test_session.add(review)
    await test_session.commit()

    await check_provider_pricelist_staleness_task(app)

    notifications = list(
        (
            await test_session.execute(
                select(AppNotification).where(AppNotification.user_id == admin.id)
            )
        ).scalars().all()
    )
    notification_types = {
        item.payload["notification_type"] for item in notifications
    }
    assert notification_types == {
        "pricelist_stale_action",
        "provider_pricelist_review",
    }
    assert all(
        item.payload["provider_config_id"] == created_pricelist_config.id
        for item in notifications
    )
