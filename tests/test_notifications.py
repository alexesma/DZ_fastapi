from types import SimpleNamespace

import pytest

from dz_fastapi.services.notifications import notify_admin_all


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
