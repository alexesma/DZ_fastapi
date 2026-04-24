from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dz_fastapi.core.time import now_moscow
from dz_fastapi.models.partner import SupplierOrderMessage
from dz_fastapi.models.settings import CustomerOrderInboxSettings
from dz_fastapi.services.scheduler import (
    _close_stale_supplier_response_messages, _notify_scheduler_issue,
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


@pytest.mark.asyncio
async def test_close_stale_supplier_response_messages(test_session):
    settings = CustomerOrderInboxSettings(
        lookback_days=1,
        mark_seen=False,
        error_file_retention_days=5,
        supplier_response_lookback_days=14,
        supplier_response_auto_close_stale_enabled=True,
        supplier_response_stale_days=7,
    )
    old_error = SupplierOrderMessage(
        provider_id=937,
        message_type='IMPORT_ERROR',
        sender_email='zakaz@cosmopart.ru',
        subject='Re: Заказ',
        received_at=now_moscow() - timedelta(days=10),
        import_error_details='Ответ распознан, но не сопоставлен',
    )
    old_retry = SupplierOrderMessage(
        provider_id=937,
        message_type='RETRY_PENDING',
        sender_email='zakaz@cosmopart.ru',
        subject='Re: Заказ',
        received_at=now_moscow() - timedelta(days=8),
    )
    fresh_error = SupplierOrderMessage(
        provider_id=937,
        message_type='IMPORT_ERROR',
        sender_email='zakaz@cosmopart.ru',
        subject='Re: Заказ',
        received_at=now_moscow() - timedelta(days=2),
    )
    test_session.add_all([settings, old_error, old_retry, fresh_error])
    await test_session.commit()

    closed_count, stale_days = await _close_stale_supplier_response_messages(
        test_session
    )
    assert stale_days == 7
    assert closed_count == 2

    rows = (
        await test_session.execute(
            select(SupplierOrderMessage)
            .where(SupplierOrderMessage.sender_email == 'zakaz@cosmopart.ru')
            .order_by(SupplierOrderMessage.id)
        )
    ).scalars().all()
    types = [row.message_type for row in rows]
    assert types == ['IGNORED', 'IGNORED', 'IMPORT_ERROR']
    assert (
        'Автозакрыто как устаревшее: старше 7 дн.'
        in (rows[0].import_error_details or '')
    )
