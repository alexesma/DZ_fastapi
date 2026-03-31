from types import SimpleNamespace

import pytest

from dz_fastapi.services.scheduler import (_notify_scheduler_issue,
                                           download_price_provider_task)


@pytest.mark.asyncio
async def test_scheduler_logs_skip(async_client, test_session, monkeypatch):
    async def fake_get_emails(session):
        return []

    monkeypatch.setattr(
        "dz_fastapi.services.scheduler.get_emails",
        fake_get_emails,
    )

    from dz_fastapi.main import app

    await download_price_provider_task(app)

    response = await async_client.get(
        "/alerts/price-check-logs", params={"limit": 5}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1


@pytest.mark.asyncio
async def test_notify_scheduler_issue_creates_admin_notification(
    monkeypatch,
):
    sent = {}
    rolled_back = {'value': False}

    class FakeSession:
        async def rollback(self):
            rolled_back['value'] = True

    async def fake_create_admin_notifications(**kwargs):
        sent.update(kwargs)
        return [SimpleNamespace(id=1)]

    monkeypatch.setattr(
        'dz_fastapi.services.scheduler.create_admin_notifications',
        fake_create_admin_notifications,
    )

    session = FakeSession()
    await _notify_scheduler_issue(
        session=session,
        subject='Broken task',
        text='Something failed',
    )

    assert rolled_back['value'] is True
    assert sent['session'] is session
    assert sent['title'] == 'Broken task'
    assert sent['message'] == 'Something failed'
    assert sent['level'] == 'error'
    assert sent['link'] == '/admin/settings'
